"""Publication safety cases reported by the independent v1.9 review."""
from datetime import timedelta

import pytest

from bridge import planning, process
from test_process import FakeOps, NOW, published, ctx, m  # shared fixtures


def test_sequence_and_maintenance_history_are_consistent():
    base = published("r", 2026092500, version=8)
    history = [{"version": 9, "operation": "VACUUM END", "userMetadata": None}, base]
    assert planning.precheck(history, 2026092500, "2026-09-25") == "ALREADY_CURRENT"
    assert planning.precheck(history, 2026092400, "2026-09-24") == "SUPERSEDED"
    with pytest.raises(ValueError, match="upstream_sequence"):
        planning.validate_sequence("2026-09-26", 2026092500)
    bad = published("r", 2026092500, version=8)
    assert planning.precheck([bad], 2026092400, "2026-09-26") == "FAILED_UPSTREAM_SEQUENCE"


def test_manual_date_rollback_ignores_failed_and_restored_versions():
    good = published("good", 2026092500, version=5, rows=10)
    bad = published("bad", 2026092501, version=6, rows=7)
    history = [{"version": 7, "operation": "RESTORE", "userMetadata": None}, bad, good]
    assert planning.rollback_candidates(history, {5: 10, 6: 7}, "2026-09-25") == []
    assert planning.rollback_candidates([bad, good], {5: 10}, "2026-09-25") == [5]


def test_log_failure_on_one_table_does_not_prevent_next(ctx, inventory):
    a, b = inventory["mappings"]

    class BrokenFirst(FakeOps):
        def log(self, **fields):
            if fields["mapping_id"] == a["mapping_id"] and fields["status"] == "STARTED":
                raise RuntimeError("UC temporarily unavailable")
            super().log(**fields)

    ops = BrokenFirst(a)
    result = process.process_batch(ctx, [a, b], ops, clock=lambda: NOW)
    assert result[a["mapping_id"]] == "FAILED_UNKNOWN"
    assert any(row["mapping_id"] == b["mapping_id"] for row in ops.logs)


def test_export_timeout_is_bounded_by_cutoff(ctx, m):
    ctx.settings["execution"]["publish_margin_minutes"] = 0
    observed = []

    class Timed(FakeOps):
        def export(self, conn, m, ext, location, timeout):
            observed.append(timeout)
            super().export(conn, m, ext, location, timeout)

    ctx.cutoff_utc = NOW + timedelta(minutes=5)
    assert process.process_table(ctx, m, Timed(m), now=NOW) == "PUBLISHED"
    assert observed == [300]
