from datetime import datetime, timezone

import pytest

from bridge import planning

UTC = timezone.utc


def md(release, seq):
    return planning.user_metadata(release, seq, "2026-09-25", "m", "1", "a")


def h(version, release=None, seq=None, rows=None, raw=None):
    return {"version": version, "operation": "WRITE",
            "userMetadata": raw if raw is not None else (md(release, seq) if release else None),
            "operationMetrics": {"numOutputRows": str(rows)} if rows is not None else {}}


def test_batches_largest_first_and_limits():
    ms = [{"mapping_id": f"m{i}", "estimated_bytes": b} for i, b in enumerate([5, 100, 1, 60, 2, 3])]
    batches = planning.make_batches(ms, max_tables=2, max_bytes=100)
    assert batches[0] == ["m1"]                       # 100 fills a batch
    assert batches[1] == ["m3", "m0"]                 # 60 + 5
    assert sum(len(b) for b in batches) == 6
    assert all(len(b) <= 2 for b in batches)


def test_oversized_table_gets_own_batch():
    ms = [{"mapping_id": "big", "estimated_bytes": 10**12}, {"mapping_id": "s", "estimated_bytes": 1}]
    assert planning.make_batches(ms, 10, 100) == [["big"], ["s"]]


@pytest.mark.parametrize("now,expected", [
    (datetime(2026, 9, 25, 15, 0, tzinfo=UTC), datetime(2026, 9, 26, 1, 0, tzinfo=UTC)),   # 01:00 AEST
    (datetime(2026, 9, 26, 0, 59, tzinfo=UTC), datetime(2026, 9, 26, 1, 0, tzinfo=UTC)),   # 10:59 local
    (datetime(2026, 9, 26, 1, 0, tzinfo=UTC), datetime(2026, 9, 27, 1, 0, tzinfo=UTC)),    # at cutoff -> next day
    (datetime(2026, 10, 4, 15, 0, tzinfo=UTC), datetime(2026, 10, 5, 0, 0, tzinfo=UTC)),   # after DST starts (AEDT)
])
def test_cutoff_in_sydney_time_including_dst(now, expected):
    assert planning.cutoff_utc(now, "11:00", "Australia/Sydney") == expected


def test_automatic_night_never_rolls_deadline_to_tomorrow():
    tz = "Australia/Sydney"
    before = datetime(2026, 9, 25, 15, 30, tzinfo=UTC)  # 01:30 Saturday
    after = datetime(2026, 9, 25, 21, 30, tzinfo=UTC)   # 07:30 Saturday
    expected = planning.business_day(before, tz)
    assert expected.isoformat() == "2026-09-26"
    assert planning.business_day(after, tz) == expected
    assert planning.deadline_utc(expected, "07:00", tz) == datetime(2026, 9, 25, 21, tzinfo=UTC)
    assert after > planning.deadline_utc(expected, "07:00", tz)
    assert planning.deadline_utc("2026-10-05", "07:00", tz) == datetime(2026, 10, 4, 20, tzinfo=UTC)


def test_missing_expected_synapse_completion_can_be_polled(inventory):
    from bridge import synapse
    inventory["synapse"]["upstream_control_schema"] = "bridge_upstream"

    class Connection:
        def __init__(self, rows):
            self.rows = rows

        def query(self, sql, *args):
            assert "business_date=CAST(? AS date)" in sql
            assert args[-1] == "2026-09-26"
            return self.rows

    assert synapse.upstream_run(Connection([]), inventory, "2026-09-26") is None
    completed = synapse.upstream_run(Connection([("run-26", "2026-09-26", 2026092600)]),
                                     inventory, "2026-09-26")
    assert completed["business_date"] == "2026-09-26"
    corrected = synapse.upstream_run(Connection([("b", "2026-09-26", 2026092601),
                                                  ("a", "2026-09-26", 2026092600)]),
                                     inventory, "2026-09-26")
    assert corrected["upstream_run_id"] == "b" and corrected["sequence"] == 2026092601
    with pytest.raises(RuntimeError, match="Conflicting COMPLETE upstream runs"):
        synapse.upstream_run(Connection([("a", "2026-09-26", 2026092601),
                                         ("b", "2026-09-26", 2026092601)]),
                             inventory, "2026-09-26")


def test_precheck():
    assert planning.precheck([], 5) is None
    assert planning.precheck([h(3, "r5", 5)], 5) == "ALREADY_CURRENT"
    assert planning.precheck([h(3, "r6", 6)], 5) == "SUPERSEDED"
    assert planning.precheck([h(3, "r4", 4)], 5) is None
    # A later unknown write cannot establish that the prior release is still live.
    assert planning.precheck([h(9), h(8, raw="not json"), h(7, raw='{"x":1}'), h(3, "r6", 6)], 5) == "FAILED_VERIFY"
    assert planning.precheck([h(9), h(3, "r6", 6)], 7) is None  # new night may repair it


def test_restore_commit_never_counts_as_published():
    restore = {"version": 9, "operation": "RESTORE", "operationMetrics": {},
               "userMetadata": planning.restore_metadata("r6", 8, 7, "why")}
    assert planning.latest_bridge_commit([restore, h(8, "r6", 6)])[1]["release_id"] == "r6"
    assert planning.precheck([restore], 7) is None
    assert planning.precheck([restore, h(8, "r6", 6)], 6) == "FAILED_VERIFY_ROLLED_BACK"
    assert planning.verify_after_write([restore, h(8, "r6", 6, rows=10)], "r6", 6, 10)[0] == \
        "FAILED_VERIFY_ROLLED_BACK"
    assert planning.precheck([restore, h(8, "r6", 6)], 7) is None


@pytest.mark.parametrize("history,expected", [
    ([h(4, "r5", 5, rows=10)], ("PUBLISHED", 4)),
    ([{**h(5), "operation": "OPTIMIZE"}, h(4, "r5", 5, rows=10)], ("PUBLISHED", 4)),
    ([h(5), h(4, "r5", 5, rows=10)], ("FAILED_VERIFY", 4)),
    ([h(4, "r5", 5, rows=9)], ("FAILED_VERIFY_NEEDS_ROLLBACK", 4)),
    ([h(4, "r6", 6, rows=10)], ("SUPERSEDED", None)),                    # Delta skipped: newer night
    ([h(4, "r4", 4, rows=10)], ("FAILED_WRITE", None)),                  # nothing committed
    ([], ("FAILED_WRITE", None)),
])
def test_verify_after_write(history, expected):
    status, version, _ = planning.verify_after_write(history, "r5", 5, 10)
    assert (status, version) == expected


def test_newer_valid_release_supersedes_historical_commit():
    old = h(8, "r5", 5, rows=10)
    newer = h(9, "r6", 6, rows=12)
    assert planning.verify_after_write([newer, old], "r5", 5, 10)[0] == "SUPERSEDED"
    assert planning.precheck([newer, old], 5) == "SUPERSEDED"
    unverified = h(10)
    assert planning.verify_after_write([unverified, newer, old], "r5", 5, 10)[0] == "FAILED_VERIFY"


def test_empty_publish_verifies_with_zero_rows():
    assert planning.verify_after_write([h(2, "r5", 5, rows=0)], "r5", 5, 0)[0] == "PUBLISHED"


def test_rollback_only_when_bad_commit_is_newest():
    assert planning.rollback_target_version([h(4), h(3)], 4) == 3
    assert planning.rollback_target_version([h(5), h(4)], 4) is None
    assert planning.rollback_target_version([h(0)], 0) is None


def test_metadata_round_trip():
    parsed = planning.parse_metadata(md("r5", 5))
    assert parsed["release_id"] == "r5" and parsed["sequence"] == 5 and parsed["tag"] == planning.BRIDGE_TAG


def test_status_sets_are_disjoint():
    sets = [planning.SUCCESS, planning.DETERMINISTIC_FAILURES, planning.TRANSIENT_FAILURES, planning.SKIPPED]
    assert sum(len(s) for s in sets) == len(set().union(*sets))


def test_summary_fails_only_on_mandatory():
    plan = [{"mapping_id": "a", "mandatory": True}, {"mapping_id": "b", "mandatory": False},
            {"mapping_id": "c", "mandatory": True}]
    s = planning.summarise(plan, {"a": "PUBLISHED", "b": "FAILED_EXPORT"})
    assert s["ok"] is False
    assert s["mandatory_failures"] == [{"mapping_id": "c", "status": "NOT_STARTED"}]
    assert s["other_failures"] == [{"mapping_id": "b", "status": "FAILED_EXPORT"}]
    assert planning.summarise(plan[:1], {"a": "SUPERSEDED"})["ok"] is True
