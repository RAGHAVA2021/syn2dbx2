"""The per-table flow against fake platform operations."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from bridge import contract, planning, process
from bridge import synapse as synapse_mod

NOW = datetime(2026, 9, 25, 16, 0, tzinfo=timezone.utc)


class FakeOps:
    def __init__(self, m, history=None, source_rows=10, parquet_rows=None, schema_hash=None,
                 commit=True, commit_rows=None, write_raises=None, export_raises=None, target_ok=True):
        self.m, self.hist = m, list(history or [])
        self.source_rows, self.parquet_rows = source_rows, source_rows if parquet_rows is None else parquet_rows
        self.hash = schema_hash
        self.commit, self.commit_rows = commit, commit_rows
        self.write_raises, self.export_raises, self.target_ok = write_raises, export_raises, target_ok
        self.read_paths = []
        self.calls, self.logs, self.restored = [], [], None

    @contextmanager
    def synapse(self):
        self.calls.append("connect")
        yield "conn"

    def schema_hash(self, conn, m):
        self.calls.append("hash")
        return self.hash or m["source_schema_hash"]

    def sql_error_info(self, exc):
        return getattr(exc, "code", None), getattr(exc, "state", None)

    classify_export_error = staticmethod(synapse_mod.classify_export_error)

    def count_rows(self, conn, m, timeout):
        self.calls.append("count")
        return self.source_rows

    def export(self, conn, m, ext, location, timeout):
        self.calls.append("export")
        contract.cetas_sql(m, SETTINGS, ext, location)   # the generated names must be valid
        if self.export_raises:
            raise self.export_raises

    def history(self, m):
        return list(self.hist)

    def check_target_schema(self, m):
        if not self.target_ok:
            raise contract.ContractError("FAILED_TARGET_SCHEMA", "differs")

    def read_export(self, path, m):
        self.calls.append("read")
        self.read_paths.append(path)
        return "df"

    def validate_and_convert(self, df, m, business_date, release, reject_ancient_timestamps=False):
        return "converted", self.parquet_rows

    def overwrite(self, m, converted, sequence, metadata):
        self.calls.append("overwrite")
        if self.commit:
            rows = self.parquet_rows if self.commit_rows is None else self.commit_rows
            self.hist.insert(0, {"version": len(self.hist), "operation": "WRITE", "userMetadata": metadata,
                                 "operationMetrics": {"numOutputRows": str(rows)}})
        if self.write_raises:
            raise self.write_raises

    def restore(self, m, version, metadata):
        self.restored = version
        self.hist.insert(0, {"version": max((h["version"] for h in self.hist), default=-1) + 1,
                             "operation": "RESTORE", "userMetadata": metadata, "operationMetrics": {}})

    def log(self, **f):
        self.logs.append(f)

    def reusable_stage(self, ctx, m):
        for event in reversed(self.logs):
            if event["status"] == "VALIDATED" and event["mapping_id"] == m["mapping_id"]:
                return event["method"], event["stage_path"]
        return None


SETTINGS = None


@pytest.fixture
def ctx(inventory):
    global SETTINGS
    SETTINGS = inventory
    return process.Context(settings=inventory, release="synapse_nightly-2026092500", sequence=2026092500,
                           business_date="2026-09-25", job_run_id="42", cutoff_utc=NOW + timedelta(hours=10))


@pytest.fixture
def m(inventory):
    return inventory["mappings"][0]


def published(release, seq, version=0, rows=10, job_run_id="1"):
    day = datetime.strptime(str(seq // 100), "%Y%m%d").date().isoformat()
    return {"version": version, "operation": "WRITE", "operationMetrics": {"numOutputRows": str(rows)},
            "userMetadata": planning.user_metadata(release, seq, day, "m", job_run_id, "a")}


def test_happy_path_publishes_and_logs(ctx, m):
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert ops.calls == ["connect", "hash", "count", "export", "read", "overwrite"]
    final = ops.logs[-1]
    assert final["status"] == "PUBLISHED" and final["source_rows"] == final["parquet_rows"] == 10
    assert final["written_rows"] == 10 and final["stage_path"].startswith(
        "abfss://staging@contosostage.dfs.core.windows.net/bridge-stage/2026-09-25/synapse_nightly-2026092500/dbo__Customer/")


def test_export_that_crosses_cutoff_is_not_published(ctx, m):
    ops = FakeOps(m)
    ticks = iter([NOW, NOW, ctx.cutoff_utc])
    assert process.process_table(ctx, m, ops, now=NOW,
                                     current_time=lambda: next(ticks)) == "SKIPPED_CUTOFF"
    assert ops.calls == ["connect", "hash", "count", "export", "read"]
    assert ops.logs[-1]["status"] == "SKIPPED_CUTOFF"


def test_each_attempt_gets_a_fresh_staging_folder(ctx, m):
    a, b = FakeOps(m), FakeOps(m)
    process.process_table(ctx, m, a, now=NOW)
    process.process_table(ctx, m, b, now=NOW)
    assert a.logs[-1]["stage_path"] != b.logs[-1]["stage_path"]


def test_orphan_cleanup_name_filter_does_not_claim_other_external_tables(m):
    attempt = "20260925T160000Z-deadbeef"
    owned = process.external_table_name(m, attempt)
    assert process.external_table_candidates([owned, "x_shared_table", "x_other_20260925T160000Z_deadbeef"], [m]) == \
        {(m["mapping_id"], attempt): owned}


@pytest.mark.parametrize("seq,expected", [(2026092500, "ALREADY_CURRENT"), (2026092600, "SUPERSEDED")])
def test_precheck_skips_export_when_target_is_current_or_newer(ctx, m, seq, expected):
    ops = FakeOps(m, history=[published(ctx.release if seq == ctx.sequence else "newer", seq)])
    assert process.process_table(ctx, m, ops, now=NOW) == expected
    assert "export" not in ops.calls and "overwrite" not in ops.calls


def test_crash_after_bad_commit_does_not_become_already_current(ctx, m):
    # The bad commit was made by an earlier attempt of THIS job run (lost ack).
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=8, rows=9, job_run_id=ctx.job_run_id)])
    ctx.table_info[m["mapping_id"]] = {"expected_rows": 10}
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY_ROLLED_BACK"
    assert ops.restored == 7
    assert "overwrite" not in ops.calls


def test_lost_ack_with_wrong_manifest_logs_confirmed_count_for_rollback(ctx, m):
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=8, rows=10)], source_rows=10)
    ctx.table_info[m["mapping_id"]] = {"expected_rows": 9}
    assert process.process_table(ctx, m, ops, now=NOW) == "ALREADY_CURRENT"
    event = ops.logs[-1]
    assert event["written_rows"] == event["source_rows"] == 10
    assert event["count_source"] == "synapse_after_manifest_mismatch"
    assert planning.rollback_candidates(ops.hist, {8: event["written_rows"]}, ctx.business_date) == [8]
    assert "overwrite" not in ops.calls


def test_ancient_cetas_timestamp_is_handed_to_the_jdbc_task(ctx, m):
    class Ancient(FakeOps):
        def validate_and_convert(self, df, m, business_date, release, reject_ancient_timestamps=False):
            if reject_ancient_timestamps:
                raise contract.AncientCetasTimestamp("FAILED_CONTRACT", "historical INT96")
            return super().validate_and_convert(df, m, business_date, release)

    ops = Ancient(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "FALLBACK_REQUIRED"
    assert ops.calls.count("export") == 1 and "overwrite" not in ops.calls
    assert ops.logs[-1]["method"] == "JDBC" and ops.logs[-1].get("error_code") is None   # timestamp reroute


def test_historical_timestamp_does_not_exhaust_cetas_error_budget(ctx, m):
    ctx.settings["execution"]["max_jdbc_fallbacks_per_batch"] = 0

    class Ancient(FakeOps):
        def validate_and_convert(self, df, mapping, business_date, release, reject_ancient_timestamps=False):
            if reject_ancient_timestamps:
                raise contract.AncientCetasTimestamp("FAILED_CONTRACT", "historical timestamp")
            return super().validate_and_convert(df, mapping, business_date, release)

    ops = Ancient(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "FALLBACK_REQUIRED"
    assert getattr(ops, "fallback_count", 0) == 0


def test_precheck_failure_detail_does_not_claim_newer_night(ctx, m):
    prior = published(ctx.release, ctx.sequence, version=1)
    hist = [{"version": 2, "operation": "RESTORE", "userMetadata": None}, prior]
    ops = FakeOps(m, history=hist)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY_ROLLED_BACK"
    assert "newer night" not in ops.logs[-1]["detail"]


def test_recovery_count_uses_full_read_only_timeout_near_cutoff(ctx, m):
    ctx.cutoff_utc = NOW + timedelta(seconds=25)
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=1)])
    timeouts = []

    def count(conn, mapping, timeout):
        timeouts.append(timeout)
        return 10

    ops.count_rows = count
    assert process.process_table(ctx, m, ops, now=NOW) == "ALREADY_CURRENT"
    assert timeouts == [ctx.settings["execution"]["export_timeout_seconds"]]


def test_manifest_mismatch_recount_uses_time_left_after_export(ctx, m):
    ctx.settings["execution"]["publish_margin_minutes"] = 0
    planned(ctx, m, expected_rows=11)
    ctx.cutoff_utc = NOW + timedelta(seconds=30)
    clock = [NOW]
    ops = FakeOps(m, source_rows=10)
    timeouts = []

    original_export = ops.export
    def export(conn, mapping, ext, location, timeout):
        original_export(conn, mapping, ext, location, timeout)
        clock[0] = NOW + timedelta(seconds=28)

    def count(conn, mapping, timeout):
        timeouts.append(timeout)
        return 10

    ops.export = export
    ops.count_rows = count
    assert process.process_table(ctx, m, ops, now=NOW, current_time=lambda: clock[0]) == "PUBLISHED"
    assert timeouts == [2]


def test_fallback_event_recovers_from_one_control_log_failure(ctx, m):
    class LogGlitch(FakeOps):
        failed_once = False
        def log(self, **fields):
            if fields["status"] == "FALLBACK_REQUIRED" and not self.failed_once:
                self.failed_once = True
                raise RuntimeError("temporary control-log conflict")
            super().log(**fields)

    ops = LogGlitch(m, export_raises=CodedError(105019))
    assert process.process_table(ctx, m, ops, now=NOW) == "FALLBACK_REQUIRED"
    assert any(r["status"] == "FALLBACK_REQUIRED" for r in ops.logs)


def test_failed_write_retry_reuses_verified_immutable_stage(ctx, m):
    ops = FakeOps(m, commit=False)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_WRITE"
    ops.commit = True
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert ops.calls.count("export") == 1
    assert ops.calls.count("read") == 2  # every attempt revalidates the staged rows


def test_schema_drift_is_deterministic_and_exports_nothing(ctx, m):
    ops = FakeOps(m, schema_hash="sha256:other")
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_SCHEMA_DRIFT"
    assert "export" not in ops.calls


def test_missing_target_is_deterministic_even_though_history_would_fail(ctx, m):
    ops = FakeOps(m, target_ok=False)
    ops.history = lambda _m: (_ for _ in ()).throw(RuntimeError("TABLE_OR_VIEW_NOT_FOUND"))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_TARGET_SCHEMA"


def test_target_schema_mismatch_blocks_before_touching_synapse(ctx, m):
    ops = FakeOps(m, target_ok=False)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_TARGET_SCHEMA"
    assert ops.calls == []


def test_empty_source_rejected_unless_allowed(ctx, m):
    ops = FakeOps(m, source_rows=0)
    assert process.process_table(ctx, m, ops, now=NOW) == "REJECTED_EMPTY"
    assert "export" not in ops.calls
    m["allow_empty"] = True
    ops = FakeOps(m, source_rows=0)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"


def test_empty_export_rejected_when_count_check_disabled(ctx, m):
    m["verify_source_count"] = False
    ops = FakeOps(m, source_rows=0)
    assert process.process_table(ctx, m, ops, now=NOW) == "REJECTED_EMPTY"
    assert "count" not in ops.calls and "overwrite" not in ops.calls


def test_count_mismatch_is_transient_and_never_writes(ctx, m):
    ops = FakeOps(m, source_rows=10, parquet_rows=9)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_COUNT_MISMATCH"
    assert "overwrite" not in ops.calls


def test_export_error_is_transient(ctx, m):
    ops = FakeOps(m, export_raises=RuntimeError("tempdb full"))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_EXPORT"
    assert "overwrite" not in ops.calls


def test_contract_failure_from_validation_is_deterministic(ctx, m):
    ops = FakeOps(m)
    ops.validate_and_convert = lambda *a, **kw: (_ for _ in ()).throw(
        contract.ContractError("FAILED_CONTRACT", "Parquet type drift"))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_CONTRACT"
    assert "overwrite" not in ops.calls


def test_lost_acknowledgement_is_recognised_as_published(ctx, m):
    # The commit happened but the client saw an error: history proves success.
    ops = FakeOps(m, write_raises=ConnectionError("driver lost"))
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"


def test_write_that_did_not_commit_is_transient(ctx, m):
    ops = FakeOps(m, commit=False, write_raises=RuntimeError("cluster lost"))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_WRITE"


def test_write_skipped_because_newer_night_committed_meanwhile(ctx, m):
    ops = FakeOps(m, commit=False)
    def newer_lands_first(*a):
        ops.hist.insert(0, published("newer", 2026092600, version=5))
    ops.overwrite = newer_lands_first
    assert process.process_table(ctx, m, ops, now=NOW) == "SUPERSEDED"


def test_wrong_committed_count_is_rolled_back(ctx, m):
    ops = FakeOps(m, history=[published("old", 2026092400, version=0)], commit_rows=7)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY_ROLLED_BACK"
    assert ops.restored == 0


def test_automatic_restore_refuses_a_newer_commit(ctx, m):
    class RacingOps(FakeOps):
        reads = 0

        def history(self, mapping):
            self.reads += 1
            if self.reads == 3:
                self.hist.insert(0, published("newer", ctx.sequence + 1, version=2, rows=10))
            return super().history(mapping)

    ops = RacingOps(m, history=[published("old", ctx.sequence - 1, version=0)], commit_rows=7)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert ops.restored is None
    assert "target changed before restore" in ops.logs[-1]["detail"]


def test_automatic_restore_does_not_claim_old_data_after_later_write(ctx, m):
    class RacingAfterRestore(FakeOps):
        def restore(self, mapping, version, metadata):
            super().restore(mapping, version, metadata)
            self.hist.insert(0, published("other-writer", ctx.sequence + 1, version=2, rows=10))

    ops = RacingAfterRestore(m, history=[published("old", ctx.sequence - 1, version=0)], commit_rows=7)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert "RESTORE outcome is not the latest target commit" in ops.logs[-1]["detail"]


def test_automatic_restore_reports_intervening_commit(ctx, m):
    class RacingBeforeRestore(FakeOps):
        def restore(self, mapping, version, metadata):
            self.hist.insert(0, published("other-writer", ctx.sequence + 1, version=2, rows=10))
            super().restore(mapping, version, metadata)

    ops = RacingBeforeRestore(m, history=[published("old", ctx.sequence - 1, version=0)], commit_rows=7)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert "RESTORE outcome is not the latest target commit" in ops.logs[-1]["detail"]


def test_cutoff_skips_without_touching_anything(ctx, m):
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=ctx.cutoff_utc) == "SKIPPED_CUTOFF"
    assert ops.calls == [] and ops.logs[-1]["status"] == "SKIPPED_CUTOFF"


def test_cutoff_does_not_hide_existing_verified_publication(ctx, m):
    ctx.table_info[m["mapping_id"]] = {"expected_rows": 10}
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=4, rows=10)])
    assert process.process_table(ctx, m, ops, now=ctx.cutoff_utc) == "ALREADY_CURRENT"
    assert "export" not in ops.calls and "overwrite" not in ops.calls


def test_unexpected_error_is_logged_as_unknown(ctx, m):
    ops = FakeOps(m)
    ops.read_export = lambda *a: 1 / 0
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_UNKNOWN"
    assert "ZeroDivisionError" in ops.logs[-1]["detail"]


def test_batch_retry_redoes_only_transient_failures(ctx, inventory):
    a, b = inventory["mappings"]
    calls = []

    class Ops(FakeOps):
        def history(self, m):
            return self.hist if m["mapping_id"] == a["mapping_id"] else []

        def export(self, conn, m, ext, location, timeout):
            calls.append(m["mapping_id"])
    ops = Ops(a, history=[published(ctx.release, ctx.sequence)])
    already = {a["mapping_id"]: "PUBLISHED", b["mapping_id"]: "FAILED_EXPORT"}
    results = process.process_batch(ctx, [a, b], ops, already=already, clock=lambda: NOW)
    assert calls == [b["mapping_id"]]
    assert results[a["mapping_id"]] == "PUBLISHED"


def test_batch_retry_detects_restored_previous_success(ctx, m):
    bad = published(ctx.release, ctx.sequence, version=8, rows=9)
    restored = {"version": 9, "operation": "RESTORE", "operationMetrics": {},
                "userMetadata": planning.restore_metadata(ctx.release, 8, 7, "automatic")}
    ops = FakeOps(m, history=[restored, bad])
    result = process.process_batch(ctx, [m], ops, already={m["mapping_id"]: "PUBLISHED"}, clock=lambda: NOW)
    assert result[m["mapping_id"]] == "FAILED_VERIFY_ROLLED_BACK"
    assert "export" not in ops.calls and "overwrite" not in ops.calls


def test_deterministic_failures_are_not_retried(ctx, inventory):
    a = inventory["mappings"][0]
    ops = FakeOps(a)
    results = process.process_batch(ctx, [a], ops, already={a["mapping_id"]: "FAILED_SCHEMA_DRIFT"})
    assert results == {a["mapping_id"]: "FAILED_SCHEMA_DRIFT"} and ops.calls == []
    assert process.transient_failures({"x": "FAILED_EXPORT", "y": "FAILED_CONTRACT", "z": "PUBLISHED"}) == ["x"]


def test_external_table_name_is_valid_for_long_ids():
    m = {"mapping_id": "a" * 128}
    name = process.external_table_name(m, process.new_attempt_id(NOW))
    assert contract.IDENT.fullmatch(name) and len(name) <= 128


# ---- v1.2: CETAS error classification ----------------------------------------

class CodedError(RuntimeError):
    def __init__(self, code):
        super().__init__(f"sql error {code}")
        self.code = code


@pytest.mark.parametrize("code", [8115, 241, 517, 8134])
def test_source_data_error_is_final_not_retried(ctx, m, code):
    ops = FakeOps(m, export_raises=CodedError(code))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_SOURCE_DATA"
    assert "overwrite" not in ops.calls
    assert process.transient_failures({m["mapping_id"]: "FAILED_SOURCE_DATA"}) == []
    # a task retry skips it
    again = FakeOps(m)
    process.process_batch(ctx, [m], again, already={m["mapping_id"]: "FAILED_SOURCE_DATA"})
    assert again.calls == []


@pytest.mark.parametrize("code", [None, 40613, 10928, -2])
def test_capacity_or_connection_errors_stay_transient_without_fallback(ctx, m, code):
    ops = FakeOps(m, export_raises=CodedError(code))
    status = process.process_table(ctx, m, ops, now=NOW)
    assert status == "FAILED_EXPORT" and f"error {code}" in ops.logs[-1]["detail"]
    assert "jdbc" not in ops.calls and ops.logs[-1]["error_code"] == code


# ---- v1.3: JDBC fallback routing (the read itself: bridge/fallback.py, test_v266/267) -------------------------------------------------------

class StatefulError(RuntimeError):
    def __init__(self, code, state=None):
        super().__init__(f"sql error {code} state {state}")
        self.code, self.state = code, state


@pytest.mark.parametrize("code", [105019, 110802, 107090])
def test_cetas_specific_failure_is_handed_to_the_jdbc_task(ctx, m, code):
    ops = FakeOps(m, export_raises=CodedError(code))
    assert process.process_table(ctx, m, ops, now=NOW) == "FALLBACK_REQUIRED"
    assert ops.calls == ["connect", "hash", "count", "export"]       # nothing read or written here
    assert [l["status"] for l in ops.logs] == ["STARTED", "FALLBACK_REQUIRED"]
    assert ops.logs[-1]["method"] == "JDBC" and ops.logs[-1]["error_code"] == code
    assert ops.fallback_count == 1                                    # uses the per-batch budget


def test_fallback_disabled_keeps_cetas_failure_transient(ctx, m):
    ctx.settings["execution"]["jdbc_fallback_enabled"] = False
    ops = FakeOps(m, export_raises=CodedError(105019))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_EXPORT"
    assert "jdbc" not in ops.calls


@pytest.mark.parametrize("state", ["08S01", "HYT00"])
def test_connection_or_timeout_sqlstate_does_not_fall_back(ctx, m, state):
    ops = FakeOps(m, export_raises=StatefulError(12345, state))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_EXPORT"
    assert "jdbc" not in ops.calls


def test_happy_path_logs_method_and_duration(ctx, m):
    ticks = iter([100.0, 142.5])
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW, monotonic=lambda: next(ticks)) == "PUBLISHED"
    assert ops.logs[-1]["method"] == "CETAS" and ops.logs[-1]["duration_seconds"] == 42.5


def test_every_log_row_uses_only_control_log_columns(ctx, inventory):
    from bridge import control
    allowed = set(control.field_names(control.TABLE_LOG))
    for code in (None, 105019):
        ops = FakeOps(inventory["mappings"][0], export_raises=CodedError(code) if code else None)
        process.process_table(ctx, inventory["mappings"][0], ops, now=NOW)
        for row in ops.logs:
            assert set(row) <= allowed, set(row) - allowed


# ---- v1.4: plan-supplied schema hash and manifest counts -----------------------

def planned(ctx, m, expected_rows=None, schema_hash="same"):
    ctx.table_info[m["mapping_id"]] = {
        "schema_hash": m["source_schema_hash"] if schema_hash == "same" else schema_hash,
        "expected_rows": expected_rows}


def test_manifest_count_means_synapse_only_sees_the_cetas(ctx, m):
    planned(ctx, m, expected_rows=10)
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert ops.calls == ["connect", "export", "read", "overwrite"]        # no hash query, no COUNT_BIG
    assert ops.logs[-1]["count_source"] == "manifest" and ops.logs[-1]["source_rows"] == 10


def test_table_missing_from_manifest_is_counted_in_synapse(ctx, m):
    planned(ctx, m, expected_rows=None)
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert ops.calls == ["connect", "count", "export", "read", "overwrite"]
    assert ops.logs[-1]["count_source"] == "synapse"


def test_manifest_zero_rows_rejected_only_after_synapse_confirms(ctx, m):
    planned(ctx, m, expected_rows=0)
    ops = FakeOps(m, source_rows=0)
    assert process.process_table(ctx, m, ops, now=NOW) == "REJECTED_EMPTY"
    assert ops.calls == ["connect", "count"]


def test_wrong_manifest_zero_does_not_block_nonempty_table(ctx, m):
    planned(ctx, m, expected_rows=0)
    ops = FakeOps(m, source_rows=10)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert ops.calls == ["connect", "count", "connect", "export", "read", "overwrite"]
    assert ops.logs[-1]["count_source"] == "synapse_after_manifest_zero"


def test_manifest_zero_confirmation_failure_is_retryable(ctx, m):
    planned(ctx, m, expected_rows=0)
    ops = FakeOps(m)
    def unavailable(conn, mapping, timeout):
        raise RuntimeError("Synapse unavailable")
    ops.count_rows = unavailable
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_EXPORT"
    assert "export" not in ops.calls


def test_wrong_manifest_count_is_verified_once_and_publishes(ctx, m):
    planned(ctx, m, expected_rows=11)             # manifest says 11; source and export really have 10
    ops = FakeOps(m, source_rows=10)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert ops.calls == ["connect", "export", "read", "connect", "count", "overwrite"]
    final = ops.logs[-1]
    assert final["count_source"] == "synapse_after_manifest_mismatch" and final["source_rows"] == 10
    assert "manifest count 11 was wrong" in final["detail"]


def test_real_mismatch_with_manifest_is_still_refused(ctx, m):
    planned(ctx, m, expected_rows=10)
    ops = FakeOps(m, source_rows=10, parquet_rows=9)     # export lost a row; Synapse agrees with manifest
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_COUNT_MISMATCH"
    assert "overwrite" not in ops.calls and "Synapse says 10" in ops.logs[-1]["detail"]


def test_failed_verification_count_is_transient(ctx, m):
    planned(ctx, m, expected_rows=11)
    ops = FakeOps(m, source_rows=10)
    calls = {"n": 0}

    def count_fails(conn, mm, timeout):
        calls["n"] += 1
        raise RuntimeError("connection reset")
    ops.count_rows = count_fails
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_COUNT_MISMATCH"
    assert "verification count failed" in ops.logs[-1]["detail"] and calls["n"] == 1


@pytest.mark.parametrize("planned_hash,message", [(None, "not found"), ("sha256:changed", "!= inventory")])
def test_plan_detected_schema_drift_never_connects(ctx, m, planned_hash, message):
    planned(ctx, m, expected_rows=10, schema_hash=planned_hash)
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_SCHEMA_DRIFT"
    assert ops.calls == [] and message in ops.logs[-1]["detail"]


def test_count_disabled_ignores_manifest_and_never_counts(ctx, m):
    m["verify_source_count"] = False
    planned(ctx, m, expected_rows=999)
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert "count" not in ops.calls and ops.logs[-1].get("count_source") is None
