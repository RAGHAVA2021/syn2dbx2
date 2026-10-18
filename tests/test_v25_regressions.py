"""v2.5 regressions: no automatic RESTORE of another run's commit, Spark-free
collection, non-blocking manifest in synapse_table mode, commit-time deadline,
configurable access-denied codes."""
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from bridge import contract, control, manifest, planning, process
from bridge import synapse as synapse_mod
from test_process import FakeOps, NOW, published

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def ctx(inventory):
    import test_process
    test_process.SETTINGS = inventory
    return process.Context(settings=inventory, release="synapse_nightly-2026092500", sequence=2026092500,
                           business_date="2026-09-25", job_run_id="42", cutoff_utc=NOW + timedelta(hours=10))


@pytest.fixture
def m(inventory):
    return inventory["mappings"][0]


def untagged(version, rows):
    return {"version": version, "operation": "WRITE", "userMetadata": None,
            "operationMetrics": {"numOutputRows": str(rows)}}


# ---- High: a rerun must not RESTORE a good commit made by another job run ---------

def test_rerun_does_not_restore_another_runs_commit_on_a_live_recount(ctx, m):
    # Published by job run 1 (10 rows). This is job run 42, hours later: no
    # manifest count, no logged publication, and the source now has 12 rows.
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=5, rows=10, job_run_id="1"),
                              untagged(4, 9)], source_rows=12)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert ops.restored is None and "overwrite" not in ops.calls and "export" not in ops.calls
    final = ops.logs[-1]
    assert "NOT restored automatically" in final["detail"] and "job run 1" in final["detail"]
    assert final["delta_version"] == 5


def test_rerun_with_wrong_manifest_does_not_restore_after_source_moved(ctx, m):
    ctx.table_info[m["mapping_id"]] = {"schema_hash": m["source_schema_hash"], "expected_rows": 11}
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=5, rows=10, job_run_id="1"),
                              untagged(4, 9)], source_rows=12)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert ops.restored is None
    assert ops.logs[-1]["count_source"] == "synapse"


def test_logged_verified_publication_beats_a_wrong_manifest(ctx, m):
    # v2.4 ignored the verified publication whenever the manifest had a count.
    ctx.table_info[m["mapping_id"]] = {"schema_hash": m["source_schema_hash"], "expected_rows": 11}
    ctx.verified_publications[m["mapping_id"]] = {"version": 5, "written_rows": 10}
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=5, rows=10, job_run_id="1")],
                  source_rows=12)
    assert process.process_table(ctx, m, ops, now=NOW) == "ALREADY_CURRENT"
    assert "count" not in ops.calls and ops.restored is None
    assert ops.logs[-1]["count_source"] == "prior_verified_publication"
    assert ops.logs[-1]["written_rows"] == 10


def test_verified_publication_must_match_the_exact_version_and_metric(ctx, m):
    ctx.verified_publications[m["mapping_id"]] = {"version": 5, "written_rows": 9}   # metric differs
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=5, rows=10, job_run_id="1")],
                  source_rows=10)
    assert process.process_table(ctx, m, ops, now=NOW) == "ALREADY_CURRENT"
    assert "count" in ops.calls and ops.logs[-1]["count_source"] == "synapse"


def test_own_runs_lost_ack_bad_commit_is_still_restored(ctx, m):
    # An earlier attempt of THIS job run committed a wrong count and died
    # before verifying: the source is still fenced, so the restore is safe.
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=5, rows=9, job_run_id="42"),
                              untagged(4, 9)], source_rows=10)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY_ROLLED_BACK"
    assert ops.restored == 4


def test_own_runs_commit_with_wrong_manifest_is_confirmed_not_restored(ctx, m):
    ctx.table_info[m["mapping_id"]] = {"schema_hash": m["source_schema_hash"], "expected_rows": 9}
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=5, rows=10, job_run_id="42")],
                  source_rows=10)
    assert process.process_table(ctx, m, ops, now=NOW) == "ALREADY_CURRENT"
    assert ops.restored is None
    assert ops.logs[-1]["count_source"] == "synapse_after_manifest_mismatch"
    assert ops.logs[-1]["source_rows"] == ops.logs[-1]["written_rows"] == 10


def test_disabled_count_verification_still_needs_a_metric(ctx, m):
    m["verify_source_count"] = False
    row = published(ctx.release, ctx.sequence, version=5, rows=10)
    row["operationMetrics"] = {}
    ops = FakeOps(m, history=[row])
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert "count" not in ops.calls


# ---- Medium: the deadline is judged by the Delta commit time ------------------------

def test_publication_logs_the_delta_commit_time(ctx, m):
    class TimedOps(FakeOps):
        def overwrite(self, mapping, converted, sequence, metadata):
            super().overwrite(mapping, converted, sequence, metadata)
            self.hist[0]["timestamp_utc"] = "2026-09-25T20:59:58.500000"
    ops = TimedOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    assert ops.logs[-1]["commit_utc"] == datetime(2026, 9, 25, 20, 59, 58, 500000)


def test_deadline_uses_commit_time_not_the_later_log_event():
    deadline = datetime(2026, 9, 25, 21, 0)                 # 07:00 Sydney (AEST)
    outcomes = {
        # committed 06:59:58, logged 07:00:03 after verification: on time
        "a": {"status": "PUBLISHED", "event_utc": deadline + timedelta(seconds=3),
              "commit_utc": deadline - timedelta(seconds=2)},
        # the INSERT OVERWRITE itself committed after the deadline: late
        "b": {"status": "PUBLISHED", "event_utc": deadline + timedelta(minutes=4),
              "commit_utc": deadline + timedelta(minutes=3)},
        # a pre-v2.5 row without a commit time falls back to the event time
        "c": {"status": "PUBLISHED", "event_utc": deadline + timedelta(seconds=1), "commit_utc": None},
        # a lost-ack commit recovered as ALREADY_CURRENT still counts by commit time
        "d": {"status": "ALREADY_CURRENT", "event_utc": deadline + timedelta(hours=1),
              "commit_utc": deadline + timedelta(minutes=1)},
        "e": {"status": "ALREADY_CURRENT", "event_utc": deadline + timedelta(hours=1), "commit_utc": None},
        "f": {"status": "FAILED_EXPORT", "event_utc": deadline + timedelta(hours=1), "commit_utc": None},
    }
    assert planning.late_publications(outcomes, deadline) == ["b", "c", "d"]


@pytest.mark.parametrize("raw,expected", [("2026-09-25T20:59:58.123456", datetime(2026, 9, 25, 20, 59, 58, 123456)),
                                          ("2026-09-25T20:59:58+00:00", datetime(2026, 9, 25, 20, 59, 58)),
                                          (None, None), ("garbage", None)])
def test_commit_utc_parsing(raw, expected):
    assert planning.commit_utc([{"version": 3, "timestamp_utc": raw}], 3) == expected
    assert planning.commit_utc([{"version": 3, "timestamp_utc": raw}], 4) is None


def test_control_log_has_commit_time():
    assert "commit_utc" in control.field_names(control.TABLE_LOG)


# ---- Medium: synapse_table mode never blocks on the manifest -----------------------

def _manifest_settings(inventory):
    inventory["upstream"] = {"completion_source": "synapse_table"}
    inventory["synapse"]["upstream_control_schema"] = "bridge_upstream"
    return inventory


@pytest.mark.parametrize("error", [PermissionError("403 AuthorizationPermissionMismatch"),
                                   UnicodeDecodeError("utf-8", b"\xff\xfe", 0, 1, "invalid start byte"),
                                   ValueError("manifest is 30000000 bytes"),
                                   FileNotFoundError("gone between listing and read"),
                                   RuntimeError("storage unavailable")])
def test_optional_manifest_problems_fall_back_to_synapse_counts(inventory, error):
    settings = _manifest_settings(inventory)
    def read_text(path):
        raise error
    mf, note = manifest.optional_counts(settings, "2026-09-25", "UP-20260925", settings["mappings"],
                                        lambda d: True, read_text)
    assert mf is None and "counting in Synapse" in note and type(error).__name__ in note


def test_optional_manifest_readiness_error_also_falls_back(inventory):
    settings = _manifest_settings(inventory)
    def ready(d):
        raise PermissionError("cannot list")
    mf, note = manifest.optional_counts(settings, "2026-09-25", "UP-20260925", settings["mappings"], ready,
                                        lambda p: "")
    assert mf is None and "PermissionError" in note


def test_optional_manifest_is_used_when_valid_and_skipped_when_absent(inventory):
    settings = _manifest_settings(inventory)
    text = ("business_date,upstream_run_id,upstream_sequence,schema,table,row_count\n"
            "2026-09-25,UP-20260925,2026092500,dbo,Customer,10\n")
    mf, note = manifest.optional_counts(settings, "2026-09-25", "UP-20260925", settings["mappings"],
                                        lambda d: True, lambda p: text)
    assert note is None and mf.counts == {settings["mappings"][0]["mapping_id"]: 10}
    mf, note = manifest.optional_counts(settings, "2026-09-25", "OTHER-RUN", settings["mappings"],
                                        lambda d: True, lambda p: text)
    assert mf is None and "ManifestError" in note
    mf, note = manifest.optional_counts(settings, "2026-09-25", "UP-20260925", settings["mappings"],
                                        lambda d: False, lambda p: text)
    assert mf is None and note.startswith("no manifest")


# ---- Medium: confirmed access-denied codes are configurable -------------------------

def test_unconfirmed_access_codes_still_fall_back_but_configured_ones_are_final():
    for code in synapse_mod.ACCESS_ERROR_CANDIDATES:
        assert synapse_mod.classify_export_error(code, None) == "CETAS"
        assert synapse_mod.classify_export_error(code, None, (code,)) == "ACCESS"
    assert synapse_mod.classify_export_error(None, None, (262,)) == "CAPACITY"


def test_configured_access_code_ends_as_source_access_without_jdbc(ctx, m):
    class CodedError(RuntimeError):
        def __init__(self, code):
            super().__init__(f"SQL error {code}")
            self.code = code

    class Ops(FakeOps):
        def classify_export_error(self, code, state):
            return synapse_mod.classify_export_error(code, state, (262,))

    ops = Ops(m, export_raises=CodedError(262))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_SOURCE_ACCESS"
    assert "jdbc" not in ops.calls and not getattr(ops, "fallback_count", 0)


@pytest.mark.parametrize("value,ok", [([], True), ([262, 297], True), ([8115], False), ([-2], False),
                                      ([207], False), (["262"], False), ([True], False), (262, False)])
def test_additional_access_codes_are_validated(inventory, value, ok):
    inventory["execution"]["additional_access_error_codes"] = value
    if ok:
        contract.validate_inventory(inventory)
    else:
        with pytest.raises(ValueError, match="additional_access_error_codes"):
            contract.validate_inventory(inventory)


# ---- Medium: the Spark-free tests must collect without PySpark ---------------------

def test_suite_collects_without_pyspark(tmp_path):
    blocker = tmp_path / "sitecustomize.py"
    blocker.write_text(textwrap.dedent("""
        import sys
        class _NoPySpark:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ModuleNotFoundError("No module named 'pyspark'", name="pyspark")
                return None
        sys.meta_path.insert(0, _NoPySpark())
    """))
    env = {**os.environ, "PYTHONPATH": str(tmp_path) + os.pathsep + os.environ.get("PYTHONPATH", "")}
    result = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider",
                             str(ROOT / "tests")], cwd=ROOT, env=env, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    assert "error" not in result.stdout.lower().splitlines()[-1], result.stdout[-2000:]
