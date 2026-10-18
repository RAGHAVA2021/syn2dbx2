"""Recovery, missing-stage, and JDBC routing regressions."""
from datetime import timedelta

import pytest

from bridge import control, manifest, process
# v2.5: this module must collect without PySpark; the Spark case lives in test_loader_spark.py.
from test_process import FakeOps, NOW, published


@pytest.fixture
def ctx(inventory):
    import test_process
    test_process.SETTINGS = inventory
    return process.Context(settings=inventory, release="synapse_nightly-2026092500", sequence=2026092500,
                           business_date="2026-09-25", job_run_id="42", cutoff_utc=NOW + timedelta(hours=10))


@pytest.fixture
def m(inventory):
    return inventory["mappings"][0]


def test_missing_export_is_never_published_as_legitimate_empty(ctx, m):
    m["allow_empty"] = True
    m["verify_source_count"] = False
    ops = FakeOps(m, source_rows=0)
    ops.read_export = lambda path, mapping: (_ for _ in ()).throw(FileNotFoundError(path))
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_UNKNOWN"
    assert "overwrite" not in ops.calls


def test_first_attempt_skips_stage_lookup_but_retry_can_reuse(ctx, m):
    class Ops(FakeOps):
        def reusable_stage(self, ctx, m):
            self.calls.append("lookup")
            return super().reusable_stage(ctx, m)
    first = Ops(m)
    assert process.process_batch(ctx, [m], first, clock=lambda: NOW)[m["mapping_id"]] == "PUBLISHED"
    assert "lookup" not in first.calls
    retry = Ops(m)
    stage = f"{ctx.settings['staging']['root']}/{process.stage_location(ctx, m, 'a')}"
    retry.logs.append({"status": "VALIDATED", "mapping_id": m["mapping_id"],
                       "method": "CETAS", "stage_path": stage})
    assert process.process_batch(ctx, [m], retry, already={m["mapping_id"]: "FAILED_WRITE"},
                                 clock=lambda: NOW)[m["mapping_id"]] == "PUBLISHED"
    assert "lookup" in retry.calls and "export" not in retry.calls


def test_prior_verified_version_saves_recount_but_only_when_exact(ctx, m):
    commit = published(ctx.release, ctx.sequence, version=8, rows=10)
    ctx.table_info[m["mapping_id"]] = {"expected_rows": None}
    ctx.verified_publications[m["mapping_id"]] = {"version": 8, "written_rows": 10}
    ctx.cutoff_utc = NOW - timedelta(seconds=1)
    good = FakeOps(m, history=[commit])
    assert process.process_table(ctx, m, good, now=NOW) == "ALREADY_CURRENT"
    assert "count" not in good.calls and good.logs[-1]["count_source"] == "prior_verified_publication"

    ctx.verified_publications[m["mapping_id"]]["version"] = 7
    mismatch = FakeOps(m, history=[commit])
    assert process.process_table(ctx, m, mismatch, now=NOW) == "ALREADY_CURRENT"
    assert "count" in mismatch.calls


def test_verified_publication_lookup_keeps_matching_source_count(inventory):
    class Result:
        def collect(self):
            return [{"mapping_id": "a", "delta_version": 5, "written_rows": 8, "source_rows": 7},
                    {"mapping_id": "a", "delta_version": 4, "written_rows": 7, "source_rows": 7}]
    class Spark:
        def sql(self, query):
            assert "status = 'PUBLISHED'" in query and "job_run_id <> 'new'" in query
            return Result()
    assert control.verified_publications(Spark(), inventory, "release", "new", ["a"]) == \
        {"a": {"version": 4, "written_rows": 7}}
    assert control.verified_publications(Spark(), inventory, "release", "new", []) == {}


def test_alert_query_separates_cetas_errors_from_timestamp_reroutes(inventory):
    class Row:
        def __getitem__(self, key):
            return {"cetas_errors": 1, "total": 13}[key]
    class Result:
        def first(self):
            return Row()
    class Spark:
        def sql(self, query):
            assert "max(error_code IS NOT NULL) AS has_code" in query
            assert "status IN ('FALLBACK_REQUIRED', 'FALLBACK_STARTED')" in query
            assert "GROUP BY mapping_id" in query
            return Result()
    counts = control.fallback_counts(Spark(), inventory, "42")
    assert counts == {"cetas_errors": 1, "timestamp_reroutes": 12, "total": 13}


@pytest.mark.parametrize("error,missing", [(FileNotFoundError("gone"), True),
                                            (RuntimeError("[PATH_NOT_FOUND] gone"), True),
                                            (RuntimeError("PERMISSION_DENIED"), False)])
def test_marker_read_only_waits_on_missing_files(error, missing):
    assert manifest.is_missing_path_error(error) is missing


def test_marker_removed_between_listing_and_read_waits(inventory):
    def read_text(path):
        if path.endswith("_SUCCESS"):
            raise FileNotFoundError(path)
        return "business_date,upstream_run_id,upstream_sequence,schema,table,row_count\n" + \
               "2026-09-25,UP-20260925,2026092500,dbo,Customer,10\n"
    parsed, note = manifest.read_certified(inventory, "2026-09-25", inventory["mappings"],
                                           read_text, manifest.is_missing_path_error)
    assert parsed is None and "disappeared" in note

    def denied(path):
        raise PermissionError(path)
    with pytest.raises(PermissionError):
        manifest.read_certified(inventory, "2026-09-25", inventory["mappings"], denied,
                                manifest.is_missing_path_error)
