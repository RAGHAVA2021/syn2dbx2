"""Recovery cutoff, publication margin and the Delta transaction guard."""
from datetime import datetime, timedelta, timezone

import pytest

from bridge import contract, planning, process
from test_process import FakeOps, NOW, ctx, m  # noqa: F401 (fixture imports)


def test_same_day_manual_recovery_is_bounded_in_sydney_time():
    before = datetime(2026, 9, 26, 0, 59, tzinfo=timezone.utc)  # 10:59 AEST
    deadline = planning.nightly_cutoff_utc("2026-09-26", before, "Australia/Sydney", "07:00", "11:00", True)
    assert deadline == datetime(2026, 9, 26, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="no rerun is permitted"):
        planning.nightly_cutoff_utc("2026-09-26", deadline, "Australia/Sydney", "07:00", "11:00", True)
    with pytest.raises(ValueError, match="historical backfills"):
        planning.nightly_cutoff_utc("2026-09-25", before, "Australia/Sydney", "07:00", "11:00", True)


def test_recovery_cutoff_observes_daylight_saving():
    before = datetime(2026, 10, 4, 23, 59, tzinfo=timezone.utc)  # 10:59 AEDT on Oct 5
    assert planning.nightly_cutoff_utc("2026-10-05", before, "Australia/Sydney", "07:00", "11:00", True) == \
        datetime(2026, 10, 5, 0, tzinfo=timezone.utc)


def test_export_is_not_started_inside_the_publication_margin(ctx, m):
    """v2.6.2: an export that could never be published is not started at all."""
    ctx.cutoff_utc = NOW + timedelta(minutes=4)          # inside the 5-minute margin
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "SKIPPED_CUTOFF"
    assert "export" not in ops.calls and "overwrite" not in ops.calls


def test_publication_margin_skips_an_export_that_finishes_inside_it(ctx, m):
    ctx.cutoff_utc = NOW + timedelta(minutes=30)
    clock = {"now": NOW}
    ops = FakeOps(m)
    real_export = ops.export

    def slow_export(*args, **kwargs):
        clock["now"] = NOW + timedelta(minutes=27)       # finishes 3 minutes before the cutoff
        return real_export(*args, **kwargs)
    ops.export = slow_export
    assert process.process_table(ctx, m, ops, current_time=lambda: clock["now"]) == "SKIPPED_CUTOFF"
    assert "export" in ops.calls and "overwrite" not in ops.calls


def test_export_timeout_is_bounded_by_the_margin_not_the_cutoff(ctx, m):
    ctx.cutoff_utc = NOW + timedelta(minutes=30)
    seen = {}
    ops = FakeOps(m)
    real_export = ops.export

    def spy(conn, mapping, ext, location, timeout):
        seen["timeout"] = timeout
        return real_export(conn, mapping, ext, location, timeout)
    ops.export = spy
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert seen["timeout"] == 25 * 60                    # 30 min to cutoff minus the 5-minute margin


def test_set_transaction_retention_is_rejected(inventory):
    inventory["table_properties"]["delta.setTransactionRetentionDuration"] = "interval 1 day"
    with pytest.raises(ValueError, match="idempotency guard"):
        contract.validate_inventory(inventory)
