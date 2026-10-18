"""v2.6.6 review fixes.

The notebook tests EXECUTE notebooks 20 and 30 against fake Spark / dbutils /
control-log objects. Earlier versions only grepped notebook text, which is how
the publish task shipped unable to publish anything.
"""
import copy
import json
import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from bridge import contract, control, fallback, planning, process
from bridge import synapse as synapse_mod


def _import_loader():
    """bridge.loader imports pyspark at module level; the Spark-free suite must still
    run these notebook tests. Without PySpark, a stub satisfies the import and is
    removed again so other tests' importorskip("pyspark") still skips correctly."""
    import importlib
    import importlib.util
    import sys
    import types
    try:
        have_pyspark = importlib.util.find_spec("pyspark") is not None
    except (ImportError, ValueError):
        have_pyspark = False
    if have_pyspark:
        return importlib.import_module("bridge.loader")
    names = ("pyspark", "pyspark.sql", "pyspark.sql.functions")
    for name in names:
        sys.modules[name] = types.ModuleType(name)
    sys.modules["pyspark"].sql = sys.modules["pyspark.sql"]
    sys.modules["pyspark.sql"].functions = sys.modules["pyspark.sql.functions"]
    try:
        return importlib.import_module("bridge.loader")
    finally:
        for name in names:
            sys.modules.pop(name, None)


loader = _import_loader()

ROOT = Path(__file__).resolve().parents[1]
NB = ROOT / "src" / "notebooks"
BUSINESS_DATE = "2026-09-27"
SEQUENCE = 2026092700
JOB_RUN = "777"


# ============================================================ notebook harness

class NotebookExit(Exception):
    pass


class Row(dict):
    def asDict(self):
        return dict(self)


class Result:
    def __init__(self, rows):
        self.rows = rows

    def first(self):
        return self.rows[0] if self.rows else None

    def collect(self):
        return self.rows


class Platform:
    """In-memory control log, Delta tables and staging storage."""

    def __init__(self, settings, plan_rows):
        self.settings, self.plan = settings, plan_rows
        self.log, self.tables, self.staged = [], {}, {}
        self.calls, self.clock = [], 0
        self.overwrite_raises = {}

    # ---- control log
    def log_table(self, spark, settings, **f):
        self.clock += 1
        self.log.append({**f, "event_utc": self.clock})

    def latest(self):
        out = {}
        for e in self.log:
            if e["release_id"] == self.plan[0]["release_id"] and e["job_run_id"] == JOB_RUN:
                out[e["mapping_id"]] = e
        return out

    def failed_fallbacks(self):
        validated = {e["mapping_id"] for e in self.log if e["status"] == "VALIDATED"}
        return {k for k, e in self.latest().items() if e.get("method") == "JDBC"
                and e["status"] in planning.TRANSIENT_FAILURES and k not in validated}

    def statuses(self, mapping_id):
        return [e["status"] for e in self.log if e["mapping_id"] == mapping_id]

    # ---- spark.sql for the notebooks' own queries
    def sql(self, query):
        if "status = 'VALIDATED') WHERE rn = 1" in query:          # 30: latest VALIDATED per mapping
            rows = {}
            for e in self.log:
                if e["status"] == "VALIDATED":
                    rows[e["mapping_id"]] = e
            return Result([Row(mapping_id=k, source_rows=v.get("source_rows"), count_source=v.get("count_source"),
                               method=v.get("method"), stage_path=v.get("stage_path")) for k, v in rows.items()])
        if "status = 'VALIDATED'" in query and "LIMIT 1" in query:  # reusable_stage
            mid = re.search(r"mapping_id = '([^']+)'", query).group(1)
            hits = [e for e in self.log if e["mapping_id"] == mid and e["status"] == "VALIDATED"]
            return Result([Row(method=hits[-1]["method"], stage_path=hits[-1]["stage_path"])] if hits else [])
        raise AssertionError(f"unexpected SQL in notebook: {query}")

    # ---- Delta + staging fakes
    def history(self, spark, m, limit=200):
        return list(self.tables.get(m["mapping_id"], []))

    def overwrite(self, spark, m, converted, sequence, metadata):
        self.calls.append(("overwrite", m["mapping_id"]))
        exc = self.overwrite_raises.pop(m["mapping_id"], None)
        if exc:
            raise exc
        hist = self.tables.setdefault(m["mapping_id"], [])
        hist.insert(0, {"version": len(hist), "operation": "WRITE", "userMetadata": metadata,
                        "operationParameters": {}, "operationMetrics": {"numOutputRows": str(converted[1])},
                        "timestamp_utc": "2026-09-27T18:00:00.000000"})

    def restore(self, spark, m, version, metadata):
        self.calls.append(("restore", m["mapping_id"], version))
        hist = self.tables[m["mapping_id"]]
        hist.insert(0, {"version": hist[0]["version"] + 1, "operation": "RESTORE", "userMetadata": metadata,
                        "operationParameters": {"version": str(version)}, "operationMetrics": {},
                        "timestamp_utc": "2026-09-27T18:05:00.000000"})

    def read_export(self, spark, path, m, list_names, require_success=False):
        names = list_names(path)
        if require_success:
            assert "_SUCCESS" in names
        return path

    def validate_and_convert(self, raw, m, business_date, release, reject_ancient_timestamps=False):
        rows = self.staged[raw.rstrip("/")]
        if isinstance(rows, Exception):
            raise rows
        return ("converted", rows), rows

    def uc_jdbc_export(self, spark, connection_name, query, target_path, **options):
        self.calls.append(("uc_jdbc_export", target_path))
        self.export_options = options
        assert connection_name == self.settings["synapse"]["uc_jdbc_connection"]
        assert query.startswith("SELECT ") and "COUNT_BIG" not in query
        self.staged[target_path.rstrip("/")] = self.jdbc_rows

    def uc_jdbc_count(self, spark, connection_name, query, timeout=None):
        self.calls.append(("uc_jdbc_count", query))
        assert "AS row_count" in query
        return self.source_rows

    # ---- dbutils
    def fs_ls(self, path):
        if path.rstrip("/") not in self.staged:
            raise FileNotFoundError(path)

        class F:
            def __init__(self, name):
                self.name = name
        return [F("_SUCCESS"), F("part-00000.parquet")]


@pytest.fixture
def platform(inventory, monkeypatch):
    settings = inventory
    settings["synapse"]["uc_jdbc_connection"] = "synapse_uc_jdbc"
    release = planning.release_id(settings["source_set_id"], SEQUENCE)
    cutoff = (datetime.now(timezone.utc) + timedelta(hours=3)).replace(tzinfo=None)
    plan = [{"job_run_id": JOB_RUN, "release_id": release, "batch_id": 0, "position": i,
             "mapping_id": m["mapping_id"], "mandatory": bool(m.get("mandatory")), "business_date": BUSINESS_DATE,
             "upstream_sequence": SEQUENCE, "cutoff_utc": cutoff, "expected_rows": 10, "count_source": "manifest",
             "source_schema_hash": m["source_schema_hash"]} for i, m in enumerate(settings["mappings"])]
    p = Platform(settings, plan)
    p.jdbc_rows = p.source_rows = 10
    monkeypatch.setattr(control, "read_plan", lambda spark, s, job, batch=None: copy.deepcopy(plan))
    monkeypatch.setattr(control, "log_table", p.log_table)
    monkeypatch.setattr(control, "latest_status",
                        lambda spark, s, rel, job: {k: v["status"] for k, v in p.latest().items()})
    monkeypatch.setattr(control, "failed_fallbacks", lambda spark, s, rel, job: p.failed_fallbacks())
    monkeypatch.setattr(control, "verified_publications", lambda *a: {})
    monkeypatch.setattr(control, "fence_released", lambda *a: False)
    for name in ("history", "overwrite", "restore", "read_export", "validate_and_convert", "uc_jdbc_export", "uc_jdbc_count"):
        monkeypatch.setattr(loader, name, getattr(p, name))
    monkeypatch.setattr(loader, "check_target_schema", lambda spark, m: None)
    monkeypatch.setattr(loader, "check_target_governance", lambda spark, m, s=None: [])
    return p


def run_notebook(platform, name, widgets):
    class DbUtils:
        class fs:
            ls = staticmethod(platform.fs_ls)
            rm = staticmethod(lambda path, recurse=False: platform.calls.append(("rm", path)))

        class notebook:
            @staticmethod
            def exit(value):
                raise NotebookExit(value)

    class Spark:
        sql = staticmethod(platform.sql)

    g = {"spark": Spark(), "dbutils": DbUtils(), "widget": lambda k, d="": str(widgets.get(k, d)),
         "load_settings": lambda: copy.deepcopy(platform.settings), "contract": contract, "control": control,
         "loader": loader, "planning": planning, "process": process, "synapse": synapse_mod,
         "__name__": "__notebook__"}
    code = (NB / name).read_text()
    try:
        exec(compile(code, str(NB / name), "exec"), g)
    except NotebookExit:
        pass
    return g


def validated(platform, mid, rows=10, count_source="manifest", method="CETAS"):
    root, prefix = platform.settings["staging"]["root"], platform.settings["staging"]["prefix"]
    path = f"{root}/{prefix}/{BUSINESS_DATE}/{platform.plan[0]['release_id']}/{mid}/20260927T010000Z-abcd0123/"
    platform.staged[path.rstrip("/")] = rows
    platform.log_table(None, None, release_id=platform.plan[0]["release_id"], job_run_id=JOB_RUN, mapping_id=mid,
                       status="VALIDATED", method=method, stage_path=path, source_rows=rows,
                       parquet_rows=rows, count_source=count_source)


PUBLISH = {"job_run_id": JOB_RUN, "batch_id": 0, "repair_count": 0}
FALLBACK = {"job_run_id": JOB_RUN, "batch_id": 0}


# ======================================= fix 1: publish reuses VALIDATED stages

def test_publish_notebook_publishes_validated_stages_without_cetas(platform):
    ids = [p["mapping_id"] for p in platform.plan]
    for mid in ids:
        validated(platform, mid)
    g = run_notebook(platform, "30_publish_batch.py", PUBLISH)
    assert g["results"] == {mid: "PUBLISHED" for mid in ids}
    assert sorted(c[1] for c in platform.calls if c[0] == "overwrite") == sorted(ids)


def test_publish_task_retry_republishes_after_a_transient_failure(platform):
    a, b = [p["mapping_id"] for p in platform.plan]
    validated(platform, a)
    validated(platform, b)
    platform.overwrite_raises[b] = RuntimeError("connection reset during commit")
    with pytest.raises(RuntimeError, match="Transient publish failures"):
        run_notebook(platform, "30_publish_batch.py", PUBLISH)
    assert platform.latest()[a]["status"] == "PUBLISHED" and platform.latest()[b]["status"] == "FAILED_WRITE"
    g = run_notebook(platform, "30_publish_batch.py", PUBLISH)   # the task retry
    assert g["results"] == {b: "PUBLISHED"}                     # b retried; a (terminal) not touched again
    assert [c for c in platform.calls if c[0] == "overwrite"].count(("overwrite", a)) == 1


def test_publish_keeps_label_of_the_proven_count(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    validated(platform, a, count_source="synapse")
    run_notebook(platform, "30_publish_batch.py", PUBLISH)
    assert platform.latest()[a]["status"] == "PUBLISHED"
    assert platform.latest()[a]["count_source"] == "synapse"


# ===================== fixes 3, 5, 6, 11: fallback notebook + module, end to end

def _require_fallback(platform, mid, status="FALLBACK_REQUIRED", code=105019):
    platform.log_table(None, None, release_id=platform.plan[0]["release_id"], job_run_id=JOB_RUN,
                       mapping_id=mid, status=status, method="JDBC", error_code=code)


def test_fallback_without_manifest_count_counts_in_synapse_and_then_publishes(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    for p in platform.plan:
        p["expected_rows"] = None
    _require_fallback(platform, a)
    g = run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)
    assert g["results"] == {a: "VALIDATED"}
    event = platform.latest()[a]
    assert (event["source_rows"], event["parquet_rows"], event["count_source"]) == (10, 10, "uc_jdbc")
    assert event["stage_path"].endswith("-jdbc/")
    assert any(c[0] == "uc_jdbc_count" for c in platform.calls)
    g = run_notebook(platform, "30_publish_batch.py", PUBLISH)
    assert g["results"] == {a: "PUBLISHED"}


def test_fallback_retries_a_mapping_left_in_fallback_started(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    _require_fallback(platform, a)
    _require_fallback(platform, a, status="FALLBACK_STARTED", code=None)   # task died mid-read
    g = run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)
    assert g["results"] == {a: "VALIDATED"}


def test_fallback_count_mismatch_stays_retryable_and_fails_the_task(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    _require_fallback(platform, a)
    platform.jdbc_rows, platform.source_rows = 9, 10
    with pytest.raises(RuntimeError, match="task retry"):
        run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)
    event = platform.latest()[a]
    # The real status (AGENTS.md: a short export is FAILED_COUNT_MISMATCH), still retryable.
    assert event["status"] == "FAILED_COUNT_MISMATCH" and "mismatch" in event["detail"]
    assert event["method"] == "JDBC"
    assert event.get("error_code") is None          # must not be counted as a CETAS failure
    platform.jdbc_rows = 10                         # the task retry re-attempts only this table
    g = run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)
    assert g["results"] == {a: "VALIDATED"}


def test_fallback_contract_failure_is_terminal_not_retried(platform):
    a, _ = [p["mapping_id"] for p in platform.plan]
    _require_fallback(platform, a)
    original = platform.uc_jdbc_export

    def export(spark, conn, query, path, **options):
        original(spark, conn, query, path, **options)
        platform.staged[path.rstrip("/")] = contract.ContractError("FAILED_CONTRACT", "conversion would lose values")
    loader.uc_jdbc_export = export
    g = run_notebook(platform, "20_serverless_jdbc_fallback.py", FALLBACK)   # no retry raised
    assert g["results"] == {a: "FAILED_CONTRACT"}
    assert platform.latest()[a]["status"] in planning.TERMINAL


class FallbackOps:
    def __init__(self, rows=10, source=10, export_raises=None, count_raises=None, convert_raises=None):
        self.rows, self.source = rows, source
        self.export_raises, self.count_raises, self.convert_raises = export_raises, count_raises, convert_raises
        self.logs, self.calls = [], []

    def export(self, m, path, timeout):
        assert timeout > 0
        self.calls.append("export")
        if self.export_raises:
            raise self.export_raises

    def count(self, m, timeout):
        self.calls.append("count")
        if self.count_raises:
            raise self.count_raises
        return self.source

    def read_export(self, path, m):
        return "df"

    def validate_and_convert(self, df, m, business_date, release):
        if self.convert_raises:
            raise self.convert_raises
        return "converted", self.rows

    def sql_error_info(self, exc):
        return synapse_mod.sql_error_info(exc)

    def classify_export_error(self, code, state):
        return synapse_mod.classify_export_error(code, state)

    def log(self, **f):
        self.logs.append(f)


def _ctx(inventory, cutoff_in=timedelta(hours=3)):
    return process.Context(settings=inventory, release="synapse_nightly-2026092700", sequence=SEQUENCE,
                           business_date=BUSINESS_DATE, job_run_id=JOB_RUN,
                           cutoff_utc=datetime.now(timezone.utc) + cutoff_in)


class SqlServerError(Exception):
    pass


@pytest.mark.parametrize("error,status", [
    (SqlServerError("com.microsoft.sqlserver.jdbc.SQLServerException: Invalid object name 'dbo.x'."),
     "FAILED_SCHEMA_DRIFT"),
    (SqlServerError("com.microsoft.sqlserver.jdbc.SQLServerException: The SELECT permission was denied on "
                    "the object 'x'."), "FAILED_SOURCE_ACCESS"),
    (SqlServerError("com.microsoft.sqlserver.jdbc.SQLServerException: Divide by zero error encountered."),
     "FAILED_SOURCE_DATA"),
    (ConnectionResetError("socket closed"), "FAILED_EXPORT"),
])
def test_fallback_classifies_read_errors(inventory, error, status):
    m = inventory["mappings"][0]
    ops = FallbackOps(export_raises=error)
    assert fallback.run_table(_ctx(inventory), m, {"expected_rows": 10}, ops) == status
    assert ops.logs[-1]["status"] == status


def test_fallback_zero_rows_is_rejected_empty(inventory):
    m = inventory["mappings"][0]
    ops = FallbackOps(rows=0, source=0)
    assert fallback.run_table(_ctx(inventory), m, {"expected_rows": 0}, ops) == "REJECTED_EMPTY"


def test_fallback_wrong_manifest_is_corrected_by_a_synapse_count(inventory):
    m = inventory["mappings"][0]
    ops = FallbackOps(rows=12, source=12)
    assert fallback.run_table(_ctx(inventory), m, {"expected_rows": 10}, ops) == "VALIDATED"
    assert ops.logs[-1]["count_source"] == "uc_jdbc_after_manifest_mismatch"
    assert ops.logs[-1]["source_rows"] == 12


def test_fallback_does_not_start_past_the_publish_deadline(inventory):
    m = inventory["mappings"][0]
    ops = FallbackOps()
    assert fallback.run_table(_ctx(inventory, cutoff_in=timedelta(minutes=2)), m, {"expected_rows": 10},
                              ops) == "SKIPPED_CUTOFF"
    assert ops.calls == []


def test_fallback_pending_includes_started():
    plan = [{"mapping_id": "a"}, {"mapping_id": "b"}, {"mapping_id": "c"}]
    latest = {"a": "FALLBACK_REQUIRED", "b": "FALLBACK_STARTED", "c": "VALIDATED"}
    assert fallback.pending(plan, latest) == ["a", "b"]


# ======================= fix 2: real mssql-python exceptions are classified

MSSQL = "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"


@pytest.mark.parametrize("state,message,kind", [
    ("42S02", MSSQL + "Invalid object name 'dbo.x'.", "SCHEMA"),
    ("42000", MSSQL + "The SELECT permission was denied on the object 'x'.", "ACCESS"),
    ("22003", MSSQL + "Arithmetic overflow error converting expression to data type int.", "DATA"),
    ("22012", MSSQL + "Divide by zero error encountered.", "DATA"),
    ("42000", MSSQL + "CREATE EXTERNAL TABLE AS SELECT failed: the path could not be used for export.", "CETAS"),
    ("08S01", "[Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: Error code 0x68", "CAPACITY"),
    ("HYT00", "[Microsoft][ODBC Driver 18 for SQL Server]Query timeout expired", "CAPACITY"),
    ("HY000", "[Microsoft][ODBC Driver 18 for SQL Server]General error", "CAPACITY"),
    ("42000", MSSQL + "The service is currently busy. Retry the request after 10 seconds.", "CAPACITY"),
])
def test_real_mssql_python_exceptions_classify(state, message, kind):
    mexc = pytest.importorskip("mssql_python.exceptions")
    with pytest.raises(Exception) as caught:
        mexc.raise_exception(state, message)
    code, sqlstate = synapse_mod.sql_error_info(caught.value)
    assert sqlstate == state
    assert synapse_mod.classify_export_error(code, sqlstate) == kind


def test_real_mssql_python_dropped_connection_is_a_plan_transient():
    mexc = pytest.importorskip("mssql_python.exceptions")
    with pytest.raises(Exception) as caught:
        mexc.raise_exception("08S01", "[Microsoft][ODBC Driver 18 for SQL Server]Communication link failure")
    assert synapse_mod.is_transient_platform_error(caught.value) is True


def test_mssql_python_shape_without_the_driver_still_yields_the_error_number():
    class ProgrammingError(Exception):     # the attributes mssql-python really sets
        def __init__(self, driver_error, ddbc_error):
            super().__init__(f"Driver Error: {driver_error}; DDBC Error: {ddbc_error}")
            self.driver_error, self.ddbc_error = driver_error, ddbc_error
    exc = ProgrammingError("Base table or view not found", "[Microsoft][SQL Server]Invalid object name 'x'.")
    assert synapse_mod.sql_error_info(exc)[0] == 208


def test_message_codes_need_sql_server_origin():
    assert synapse_mod.code_from_message("Invalid object name in my own Python code") is None
    assert synapse_mod.code_from_message("[SQL Server]Invalid object name 'x'.") == 208
    assert synapse_mod.code_from_message(
        "[SQL Server]User does not have permission to perform this action.") == 15247
    assert synapse_mod.code_from_message(
        "[SQL Server]The user does not have permission to perform this action.") == 297


def test_server_sqlstate_without_number_is_cetas_but_no_state_still_retries():
    assert synapse_mod.classify_export_error(None, "42000") == "CETAS"
    assert synapse_mod.classify_export_error(None, None) == "CAPACITY"
    assert synapse_mod.classify_export_error(None, "HY000") == "CAPACITY"


# ============================================================ fix 4: COUNT alias

def test_count_sql_names_its_column(inventory):
    assert contract.count_sql(inventory["mappings"][0]).startswith("SELECT COUNT_BIG(*) AS row_count FROM ")


# ================================================ fix 7: fallback_counts query

def test_fallback_counts_uses_both_statuses_per_mapping(inventory):
    seen = {}

    class Spark:
        def sql(self, query):
            seen["q"] = query
            return Result([Row(total=3, cetas_errors=1)])
    assert control.fallback_counts(Spark(), inventory, "42") == {"cetas_errors": 1, "timestamp_reroutes": 2,
                                                                 "total": 3}
    assert "'FALLBACK_REQUIRED', 'FALLBACK_STARTED'" in seen["q"] and "GROUP BY mapping_id" in seen["q"]


# ============================ fix 8: deferred CETAS failure handled exactly once

class CetasFails(Exception):
    code, state = 105019, "42000"


class ExtractOps(FallbackOps):
    def __init__(self, inventory):
        super().__init__()
        self.inventory, self.hist = inventory, []

    @contextmanager
    def synapse(self):
        yield "conn"

    def export(self, conn, m, ext, location, timeout):
        self.calls.append("cetas")
        raise CetasFails("CETAS path failure")

    def sql_error_info(self, exc):
        return getattr(exc, "code", None), getattr(exc, "state", None)

    def history(self, m):
        return []

    def check_target_schema(self, m):
        pass


def test_deferred_cetas_failure_logs_once_and_retry_does_not_rerun_cetas(inventory):
    m = inventory["mappings"][0]
    ctx = _ctx(inventory)
    ctx.table_info = {m["mapping_id"]: {"schema_hash": m["source_schema_hash"], "expected_rows": 10}}
    ops = ExtractOps(inventory)
    assert process.process_table(ctx, m, ops, validate_only=True) == "FALLBACK_REQUIRED"
    assert [e["status"] for e in ops.logs] == ["STARTED", "FALLBACK_REQUIRED"]
    assert ops.logs[-1]["error_code"] == 105019
    retry = process.process_batch(ctx, [m], ops, already={m["mapping_id"]: "FALLBACK_REQUIRED"}, validate_only=True)
    assert retry == {m["mapping_id"]: "FALLBACK_REQUIRED"}
    assert ops.calls.count("cetas") == 1


# ============================================ fixes 9, 12: job definition

def test_every_nightly_task_and_acceptance_run_on_serverless():
    jobs = yaml.safe_load((ROOT / "databricks.yml").read_text())["resources"]["jobs"]
    for t in jobs["nightly_full_refresh"]["tasks"]:
        inner = t.get("for_each_task", {}).get("task", t)
        assert inner.get("environment_key") == "serverless_sql", t["task_key"]
        assert "job_cluster_key" not in inner
    acceptance = jobs["acceptance"]
    assert "job_clusters" not in acceptance
    assert acceptance["tasks"][0]["environment_key"] == "serverless_sql"
    deps = acceptance["environments"][0]["spec"]["dependencies"]
    assert deps == jobs["nightly_full_refresh"]["environments"][0]["spec"]["dependencies"]


def test_acceptance_probes_confs_governance_detection_and_uc_jdbc():
    text = (NB / "acceptance_checks.py").read_text()
    for conf in ("loader.TXN_APP_ID", "loader.TXN_VERSION", "loader.USER_METADATA",
                 "spark.sql.sources.partitionOverwriteMode", "spark.sql.parquet.int96RebaseModeInRead"):
        assert conf in text
    assert "SET ROW FILTER" in text and "SET MASK" in text
    assert "loader.uc_jdbc_export(spark, connection_name" in text and "1899-12-31" in text
    assert "partition_column=" in text and "variant_4a_problems" in text   # no silent skip
    assert "synapse.connect(spark, settings, get_props)" in text and "CREATE EXTERNAL TABLE" in text


# ============================================ fix 10: USE CONNECTION grant

def test_uc_setup_grants_use_connection(inventory):
    import render_uc_setup
    inventory["synapse"]["uc_jdbc_connection"] = "synapse_uc_jdbc"
    sql = render_uc_setup.render(inventory, "job-sp")
    assert "GRANT USE CONNECTION ON CONNECTION `synapse_uc_jdbc` TO `job-sp`;" in sql


# ============================================ fix 14: password escaping

@pytest.mark.parametrize("secret", ["plain", "p;w", "a}b", "{x}", "k=v;Server=evil"])
def test_odbc_value_round_trips_through_the_driver_parser(secret):
    parser_mod = pytest.importorskip("mssql_python.connection_string_parser")
    cs = "Server=tcp:a,1433;UID=" + synapse_mod.odbc_value("u") + ";PWD=" + synapse_mod.odbc_value(secret) + ";"
    parsed = parser_mod._ConnectionStringParser()._parse(cs)
    assert parsed["pwd"] == secret and parsed["server"] == "tcp:a,1433"


def test_odbc_value_quotes_braces():
    assert synapse_mod.odbc_value("a}b;") == "{a}}b;}"


# ============== second review: proven counts are never recounted in publish

def _own_commit(platform, mid, rows):
    md = planning.user_metadata(platform.plan[0]["release_id"], SEQUENCE, BUSINESS_DATE, mid, JOB_RUN, "a", 0)
    platform.tables[mid] = [
        {"version": 5, "operation": "WRITE", "userMetadata": md, "operationParameters": {},
         "operationMetrics": {"numOutputRows": str(rows)}, "timestamp_utc": "2026-09-27T18:00:00.000000"},
        {"version": 4, "operation": "WRITE", "userMetadata": None, "operationParameters": {},
         "operationMetrics": {}, "timestamp_utc": "2026-09-26T18:00:00.000000"}]


@pytest.mark.parametrize("committed_rows,status", [(10, "ALREADY_CURRENT"), (9, "FAILED_VERIFY_ROLLED_BACK")])
def test_publish_retry_verifies_a_lost_ack_commit_without_synapse(platform, committed_rows, status):
    """The worker died after the Delta commit, before logging it. The retry must judge
    the commit with the proven VALIDATED count; publish has no Synapse access."""
    a, _ = [p["mapping_id"] for p in platform.plan]
    validated(platform, a, rows=10)
    platform.log_table(None, None, release_id=platform.plan[0]["release_id"], job_run_id=JOB_RUN,
                       mapping_id=a, status="STARTED")
    _own_commit(platform, a, committed_rows)
    g = run_notebook(platform, "30_publish_batch.py", PUBLISH)
    assert g["results"] == {a: status}
    assert not [c for c in platform.calls if c[0] == "overwrite"]
    if status == "FAILED_VERIFY_ROLLED_BACK":
        assert ("restore", a, 4) in platform.calls


def test_fallback_batch_isolates_a_table_whose_logging_fails(inventory):
    a, b = [m["mapping_id"] for m in inventory["mappings"]]

    class Ops(FallbackOps):
        def log(self, **f):
            if f["mapping_id"] == a and f["status"] == "FALLBACK_STARTED":
                raise IOError("control table briefly unavailable")
            super().log(**f)
    plan = [{"mapping_id": a, "expected_rows": 10}, {"mapping_id": b, "expected_rows": 10}]
    results = fallback.run_batch(_ctx(inventory), plan, {m["mapping_id"]: m for m in inventory["mappings"]},
                                 {a: "FALLBACK_REQUIRED", b: "FALLBACK_REQUIRED"}, Ops())
    assert results == {a: "FAILED_UNKNOWN", b: "VALIDATED"}
    assert fallback.retry_needed(results) == [a]


def test_fallback_batch_reports_planned_mapping_missing_from_inventory(inventory):
    with pytest.raises(RuntimeError, match="not in the deployed inventory"):
        fallback.run_batch(_ctx(inventory), [{"mapping_id": "gone"}], {}, {"gone": "FALLBACK_REQUIRED"},
                           FallbackOps())


def test_ci_installs_the_serverless_driver_so_real_driver_tests_run():
    ci = yaml.safe_load((ROOT / ".github" / "workflows" / "ci-cd.yml").read_text())
    install = next(s["run"] for s in ci["jobs"]["test"]["steps"] if s.get("name") == "Install test dependencies")
    assert "mssql-python==1.15.0" in install
