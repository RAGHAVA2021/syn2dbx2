"""Regression tests for the v2.6.7 review fixes."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from bridge import contract, fallback, planning, process
from bridge import synapse as synapse_mod
from test_process import FakeOps, NOW
from test_v266_regressions import (FALLBACK, PUBLISH, FallbackOps, _ctx, _require_fallback,  # noqa: F401
                                   platform, run_notebook, validated)

ROOT = Path(__file__).resolve().parents[1]
MSSQL = "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"


def _mssql_error(state, text):
    mexc = pytest.importorskip("mssql_python.exceptions")
    return mexc.sqlstate_to_exception(state, MSSQL + text)


class RealErrorOps(FakeOps):
    sql_error_info = staticmethod(synapse_mod.sql_error_info)


@pytest.fixture
def deferred(inventory):
    import test_process
    test_process.SETTINGS = inventory
    ctx = process.Context(settings=inventory, release="synapse_nightly-2026092500", sequence=2026092500,
                          business_date="2026-09-25", job_run_id="42", cutoff_utc=NOW + timedelta(hours=10))
    return ctx, inventory["mappings"][0]


# ---- 1: the bundle has no duplicate keys ---------------------------------------------

def test_bundle_has_no_duplicate_mapping_keys():
    class Strict(yaml.SafeLoader):
        pass

    def mapping(loader, node, deep=False):
        keys = [loader.construct_object(k, deep=deep) for k, _ in node.value]
        dupes = sorted({k for k in keys if keys.count(k) > 1})
        assert not dupes, f"duplicate key(s) {dupes} at line {node.start_mark.line + 1}"
        return yaml.SafeLoader.construct_mapping(loader, node, deep)
    Strict.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    yaml.load((ROOT / "databricks.yml").read_text(), Strict)


# ---- 4: a CETAS error without an error number still counts as a CETAS error ----------

def test_serverless_cetas_error_without_number_is_logged_as_cetas_error(deferred):
    ctx, m = deferred
    err = _mssql_error("42000", "External file access failed due to internal error: HdfsBridge_IsDirExist")
    ops = RealErrorOps(m, export_raises=err)
    assert process.process_table(ctx, m, ops, now=NOW, validate_only=True) == "FALLBACK_REQUIRED"
    event = ops.logs[-1]
    # NULL is reserved for timestamp reroutes (control.fallback_counts).
    assert event["error_code"] == process.CETAS_ERROR_WITHOUT_NUMBER is not None
    assert "SQLSTATE 42000" in event["detail"] and "error None" not in event["detail"]


def test_timestamp_reroute_keeps_a_null_code(deferred):
    ctx, m = deferred

    class Ancient(FakeOps):
        def validate_and_convert(self, df, mapping, day, release, reject_ancient_timestamps=False):
            if reject_ancient_timestamps:
                raise contract.AncientCetasTimestamp("FAILED_CONTRACT", "historical timestamp")
            return super().validate_and_convert(df, mapping, day, release)
    ops = Ancient(m)
    assert process.process_table(ctx, m, ops, now=NOW, validate_only=True) == "FALLBACK_REQUIRED"
    assert ops.logs[-1].get("error_code") is None


# ---- 5: SQLSTATE class 22 is a data error, never a fallback ---------------------------

@pytest.mark.parametrize("state,text", [
    ("22018", "Conversion failed when converting from a character string to uniqueidentifier."),
    ("22003", "Some future overflow message the package does not know."),
])
def test_sqlstate_class_22_is_data_and_final(deferred, state, text):
    ctx, m = deferred
    ops = RealErrorOps(m, export_raises=_mssql_error(state, text))
    assert process.process_table(ctx, m, ops, now=NOW, validate_only=True) == "FAILED_SOURCE_DATA"
    assert getattr(ops, "fallback_count", 0) == 0


def test_sqlstate_class_22_without_number_classifies_as_data():
    assert synapse_mod.classify_export_error(None, "22018") == "DATA"
    assert synapse_mod.classify_export_error(None, "42000") == "CETAS"


# ---- 3 and 6: the UC-JDBC fallback is deadline-bounded and uses the read options -----

def test_fallback_read_timeout_is_bounded_by_the_publication_deadline(inventory):
    m = inventory["mappings"][0]
    seen = {}

    class Ops(FallbackOps):
        def export(self, mapping, path, timeout):
            seen["timeout"] = timeout
            super().export(mapping, path, timeout)
    fallback.run_table(_ctx(inventory, cutoff_in=timedelta(minutes=20)), m, {"expected_rows": 10}, Ops())
    margin = inventory["execution"].get("publish_margin_minutes", 5)
    assert 0 < seen["timeout"] <= (20 - margin) * 60


def test_fallback_that_reaches_the_deadline_mid_table_is_skipped(inventory):
    m = inventory["mappings"][0]
    start = datetime.now(timezone.utc)
    ticks = iter([start, start + timedelta(hours=5)])   # export, then the count would start too late
    ops = FallbackOps(rows=12, source=12)                     # manifest disagrees -> count needed
    assert fallback.run_table(_ctx(inventory), m, {"expected_rows": 10}, ops,
                              now=start, clock=lambda: next(ticks)) == "SKIPPED_CUTOFF"
    assert "count" not in ops.calls


def test_fallback_notebook_passes_partitioning_fetch_size_and_timeout(platform):
    platform.settings["mappings"][1]["jdbc_partition_column"] = next(
        c["name"] for c in platform.settings["mappings"][1]["columns"]
        if c["sql_type"] in contract.PARTITIONABLE_SQL_TYPES)
    b = platform.plan[1]["mapping_id"]
    _require_fallback(platform, b)
    import bridge.loader as loader_mod
    loader_mod.uc_jdbc_row = lambda spark, conn, query, timeout=None: ("1", "100")
    run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)
    opts = platform.export_options
    assert opts["bounds"] == ("1", "100") and opts["partition_column"]
    assert opts["num_partitions"] == contract.execution(platform.settings, "jdbc_num_partitions")
    assert opts["fetch_size"] and 0 < opts["timeout"] <= platform.settings["execution"]["export_timeout_seconds"]


def test_bounds_sql_names_its_columns(inventory):
    m = inventory["mappings"][1]
    m["jdbc_partition_column"] = next(c["name"] for c in m["columns"]
                                      if c["sql_type"] in contract.PARTITIONABLE_SQL_TYPES)
    sql = contract.bounds_sql(m)
    assert " AS lo, " in sql and " AS hi FROM " in sql


# ---- 7: when both routes fail the table ends with a real failure status --------------

def test_failed_fallback_ends_failed_export_and_a_repair_retries_it(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    _require_fallback(platform, a)
    import bridge.loader as loader_mod

    def broken(*args, **kwargs):
        raise ConnectionResetError("socket closed")
    real = loader_mod.uc_jdbc_export
    loader_mod.uc_jdbc_export = broken
    for _ in range(2):   # first attempt + task retry
        with pytest.raises(RuntimeError, match="task retry"):
            run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)
    final = platform.latest()[a]
    assert final["status"] == "FAILED_EXPORT" and final["method"] == "JDBC"
    summary = planning.summarise([{"mapping_id": a, "mandatory": True}], {a: final["status"]})
    assert summary["mandatory_failures"] == [{"mapping_id": a, "status": "FAILED_EXPORT"}]
    loader_mod.uc_jdbc_export = real          # a Repair run of the fallback task now succeeds
    g = run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)
    assert g["results"] == {a: "VALIDATED"}


def test_publish_failures_are_not_mistaken_for_failed_fallbacks(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    validated(platform, a, method="JDBC")
    platform.log_table(None, None, release_id=platform.plan[0]["release_id"], job_run_id="777",
                       mapping_id=a, status="FAILED_WRITE", method="JDBC")
    assert platform.failed_fallbacks() == set()


# ---- 8: 4A settings are enforced before deploy and UC setup --------------------------

@pytest.mark.parametrize("change", ["no_connection", "table_completion"])
def test_deploy_and_setup_refuse_an_inventory_without_the_4a_settings(inventory, tmp_path, change):
    import prepare_deploy
    import render_uc_setup
    if change == "no_connection":
        inventory["synapse"].pop("uc_jdbc_connection")
    else:
        inventory["upstream"] = {"completion_source": "synapse_table"}
        inventory["synapse"]["upstream_control_schema"] = "bridge_upstream"
    with pytest.raises(ValueError):
        render_uc_setup.render(inventory, "job-sp")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "inventory.dev.json").write_text(json.dumps(inventory))
    (tmp_path / "databricks.yml").write_text(
        "targets:\n  dev:\n    workspace:\n      host: https://adb-1.azuredatabricks.net\n")
    with pytest.raises((SystemExit, ValueError)):
        prepare_deploy.prepare("dev", root=tmp_path)


# ---- 10: a reused stage's whole release folder is cleaned after publication ----------

def test_publish_from_a_reused_jdbc_stage_deletes_the_cetas_leftover(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    validated(platform, a, method="JDBC")
    run_notebook(platform, "30_publish_batch.py", PUBLISH)
    st = platform.settings["staging"]
    prefix = f"{st['root']}/{st['prefix']}/2026-09-27/{platform.plan[0]['release_id']}/{a}/"
    assert ("rm", prefix) in platform.calls


# ---- 12: a missing VALIDATED stage says so -------------------------------------------

def test_publish_reports_a_missing_validated_stage_clearly(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    validated(platform, a)
    platform.staged.clear()           # lifecycle removed the stage
    with pytest.raises(RuntimeError, match="Transient publish failures"):
        run_notebook(platform, "30_publish_batch.py", PUBLISH)
    event = platform.latest()[a]
    assert event["status"] == "FAILED_EXPORT" and "no longer exists" in event["detail"]
    assert "CETAS failed" not in event["detail"]
