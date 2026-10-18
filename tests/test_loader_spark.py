"""Loader logic on a real local Spark session (skipped when pyspark is not installed).

Delta itself is not available locally, so the idempotent-write behaviour is
proven on the platform by src/notebooks/acceptance_checks.py. Here the target
is a local Parquet table, which checks the generated SQL, schema checks and
session-setting hygiene.
"""
import datetime as dt
import decimal
import os

import pytest

pyspark = pytest.importorskip("pyspark")
pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from bridge import contract, control, loader, planning  # noqa: E402


def test_governed_targets_fail_before_export(inventory):
    m = inventory["mappings"][0]

    class Result:
        def __init__(self, rows): self.rows = rows
        def collect(self): return self.rows
        def first(self): return self.rows[0] if self.rows else None

    class Conf:
        def __init__(self, version): self.version = version
        def get(self, key, default=None):
            return self.version if key == loader.RUNTIME_VERSION_CONF else default

    class Spark:
        def __init__(self, policy=(), direct=None, version="18.x-scala2.13"):
            self.policy, self.direct = policy, direct or {}
            self.conf = Conf(version)
        def sql(self, statement):
            if statement.startswith("SHOW EFFECTIVE POLICIES"):
                return Result(self.policy)
            import json
            return Result([(json.dumps({"columns": [{"name": "id"}], **self.direct}),)])

    loader.check_target_governance(Spark(), m)
    with pytest.raises(contract.ContractError, match="governed policies"):
        loader.check_target_governance(Spark(policy=[{"Policy Type": "ROW_FILTER", "Policy Name": "p"}]), m)
    with pytest.raises(contract.ContractError, match="governed policies"):
        loader.check_target_governance(Spark(direct={"row_filter": {"filter_function": {"function_name": "f"}}}), m)
    with pytest.raises(contract.ContractError, match="governed policies"):
        loader.check_target_governance(Spark(direct={"column_masks": [{"column_name": "pii"}]}), m)

    class Inaccessible(Spark):
        def sql(self, statement):
            raise RuntimeError("READ METADATA denied")
    with pytest.raises(contract.ContractError, match="cannot inspect governance"):
        loader.check_target_governance(Inaccessible(), m)

    class Malformed(Spark):
        def sql(self, statement):
            return Result([("not JSON",)]) if statement.startswith("DESCRIBE") else Result([])
    with pytest.raises(contract.ContractError, match="cannot inspect governance"):
        loader.check_target_governance(Malformed(), m)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    wh = tmp_path_factory.mktemp("warehouse")
    s = (SparkSession.builder.master("local[2]").appName("bridge-tests")
         .config("spark.sql.warehouse.dir", str(wh)).config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "true").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def customer_table(ntz=False, order=None, region_type=pa.int32()):
    ts_type = pa.timestamp("us") if ntz else pa.timestamp("us", tz="UTC")
    cols = {
        "customer_id": pa.array([1, 2], pa.int64()),
        "customer_name": pa.array(["Ann", None], pa.string()),
        "segment_code": pa.array(["A   ", "B   "], pa.string()),
        "is_active": pa.array([True, False], pa.bool_()),
        "credit_limit": pa.array([decimal.Decimal("10.5000"), None], pa.decimal128(19, 4)),
        "created_at": pa.array([dt.datetime(2026, 9, 25, 0, 30), dt.datetime(2026, 9, 25, 23, 59, 59, 123456)], ts_type),
        "region_id": pa.array([7, None], region_type),
    }
    names = order or list(cols)
    return pa.table({n: cols[n] for n in names})


def test_non_spark_ancient_parquet_uses_proleptic_gregorian(spark, tmp_path):
    """CETAS-like Parquet has no Spark writer metadata for calendar rebasing."""
    d = tmp_path / "ancient"
    d.mkdir()
    pq.write_table(pa.table({"d": pa.array([dt.date(1, 1, 1)], pa.date32()),
                             "ts": pa.array([dt.datetime(1753, 1, 1)], pa.timestamp("us"))}),
                   d / "part.parquet", use_deprecated_int96_timestamps=True)
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    row = spark.read.parquet(str(d)).first()
    assert row["d"] == dt.date(1, 1, 1)
    m = {"mapping_id": "ancient", "columns": [
        {"name": "d", "sql_type": "date", "max_length": 3, "precision": 10, "scale": 0, "nullable": True},
        {"name": "ts", "sql_type": "datetime", "max_length": 8, "precision": 23, "scale": 3,
         "nullable": True}]}
    # This writer's INT96 timestamp actually shifts by hours on local Spark.
    # Do not let a successful read silently publish that shifted value.
    with pytest.raises(loader.AncientCetasTimestamp):
        loader.validate_and_convert(spark.read.parquet(str(d)), m, "2026-09-25", "r",
                                    reject_ancient_timestamps=True)


def write_export(tmp_path, table, name="part-0.parq"):
    d = tmp_path / "export"
    d.mkdir(exist_ok=True)
    pq.write_table(table, d / name)
    (d / "_SUCCESS").write_text("")
    return str(d)


def names(d):
    return os.listdir(d)


def test_latest_status_is_scoped_to_one_job_run(spark):
    settings = {"control": {"catalog": "spark_catalog", "schema": "default"}}
    t = "`spark_catalog`.`default`.`table_log`"
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    try:
        spark.sql(f"CREATE TABLE {t} (release_id STRING, job_run_id STRING, "
                  "mapping_id STRING, status STRING, event_utc TIMESTAMP, commit_utc TIMESTAMP) USING PARQUET")
        spark.sql(f"INSERT INTO {t} VALUES "
                  "('release1','job1','A','FAILED_SCHEMA_DRIFT',TIMESTAMP'2026-09-25 01:00:00',NULL),"
                  "('release1','job2','A','PUBLISHED',TIMESTAMP'2026-09-25 02:00:00',NULL),"
                  "('release1','job2','A','ALREADY_CURRENT',TIMESTAMP'2026-09-25 03:00:00',"
                  "TIMESTAMP'2026-09-25 01:59:00')")
        assert control.latest_status(spark, settings, "release1", "job1") == {"A": "FAILED_SCHEMA_DRIFT"}
        assert control.latest_status(spark, settings, "release1", "job2") == {"A": "ALREADY_CURRENT"}
        outcome = control.latest_outcomes(spark, settings, "release1", "job2")["A"]
        assert outcome["commit_utc"] == dt.datetime(2026, 9, 25, 1, 59)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {t}")


def test_history_expands_when_maintenance_fills_bounded_window(inventory):
    class Result:
        def __init__(self, rows): self.rows = rows
        def select(self, *cols): return self      # project_history; the columns are checked on real Spark below
        def collect(self): return self.rows
    class Spark:
        def __init__(self): self.calls = []
        def sql(self, query):
            self.calls.append(query)
            maintenance = [{"version": i + 1, "operation": "OPTIMIZE", "userMetadata": None,
                            "operationParameters": {}, "operationMetrics": {}, "timestamp_utc": None}
                           for i in range(2)]
            published = {"version": 0, "operation": "WRITE", "userMetadata":
                         planning.user_metadata("rel-1", 2026092500, "2026-09-25", "m", "1", "a"),
                         "operationParameters": {}, "operationMetrics": {"numOutputRows": "1"},
                         "timestamp_utc": "2026-09-25T20:59:58.000000"}
            return Result(maintenance if "LIMIT" in query else maintenance + [published])
    s = Spark()
    assert len(loader.history(s, inventory["mappings"][0], limit=2)) == 3
    assert len(s.calls) == 2


def test_valid_export_converts_to_target_types(spark, tmp_path, inventory):
    m = inventory["mappings"][0]
    raw = loader.read_export(spark, write_export(tmp_path, customer_table()), m, names)
    df, rows = loader.validate_and_convert(raw, m, "2026-09-25", "rel-1")
    assert rows == 2
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert types == contract.target_schema(m)
    got = {r["customer_id"]: r for r in df.collect()}
    # datetime2 wall clock survives exactly (no time-zone shift) as timestamp_ntz
    assert got[1]["created_at"] == dt.datetime(2026, 9, 25, 0, 30)
    assert got[2]["created_at"] == dt.datetime(2026, 9, 25, 23, 59, 59, 123456)
    assert got[1]["segment_code"] == "A   "             # char padding preserved
    assert got[1]["_bridge_business_date"] == dt.date(2026, 9, 25) and got[1]["_bridge_release_id"] == "rel-1"


def test_timestamp_written_without_utc_flag_is_also_accepted(spark, tmp_path, inventory):
    m = inventory["mappings"][0]
    raw = loader.read_export(spark, write_export(tmp_path, customer_table(ntz=True)), m, names)
    assert dict((f.name, f.dataType.simpleString()) for f in raw.schema.fields)["created_at"] == "timestamp_ntz"
    df, _ = loader.validate_and_convert(raw, m, "2026-09-25", "rel-1")
    assert df.filter("customer_id = 1").first()["created_at"] == dt.datetime(2026, 9, 25, 0, 30)


def test_column_order_drift_is_a_contract_failure(spark, tmp_path, inventory):
    m = inventory["mappings"][0]
    order = ["customer_name", "customer_id", "segment_code", "is_active", "credit_limit", "created_at", "region_id"]
    raw = loader.read_export(spark, write_export(tmp_path, customer_table(order=order)), m, names)
    with pytest.raises(contract.ContractError) as e:
        loader.validate_and_convert(raw, m, "2026-09-25", "r")
    assert e.value.status == "FAILED_CONTRACT"


def test_parquet_type_drift_is_a_contract_failure(spark, tmp_path, inventory):
    m = inventory["mappings"][0]
    raw = loader.read_export(spark, write_export(tmp_path, customer_table(region_type=pa.int64())), m, names)
    with pytest.raises(contract.ContractError, match="region_id: Parquet type bigint"):
        loader.validate_and_convert(raw, m, "2026-09-25", "r")


def test_empty_export_folder_gives_empty_frame_with_contract_schema(spark, tmp_path, inventory):
    m = inventory["mappings"][0]
    d = tmp_path / "empty"
    d.mkdir()
    (d / "_SUCCESS").write_text("")
    raw = loader.read_export(spark, str(d), m, names)
    df, rows = loader.validate_and_convert(raw, m, "2026-09-25", "r")
    assert rows == 0 and df.count() == 0
    assert {f.name: f.dataType.simpleString() for f in df.schema.fields} == contract.target_schema(m)


def test_lossy_conversion_is_counted_not_thrown_under_ansi(spark, monkeypatch):
    # Force a lossy contract to prove try_cast reports losses instead of throwing.
    m = {"columns": [{"name": "v", "sql_type": "x"}]}
    monkeypatch.setattr(contract, "columns",
                        lambda _m: [{"name": "v", "parquet_type": "string", "target_type": "int"}])
    raw = spark.createDataFrame([("1",), ("abc",), (None,)], "v string")
    with pytest.raises(contract.ContractError, match=r"lose non-null values: \{'v': 1\}"):
        loader.validate_and_convert(raw, m, "2026-09-25", "r")


def test_overwrite_sql_and_target_schema_check_on_a_real_table(spark, tmp_path, inventory):
    m = dict(inventory["mappings"][0], target_catalog="spark_catalog", target_schema="default",
             target_table="customer_it")
    cols = ", ".join(f"`{n}` {t}" for n, t in contract.target_schema(m).items())
    spark.sql(f"CREATE TABLE {contract.fq_target(m)} ({cols}) USING parquet")
    loader.check_target_schema(spark, m)                       # exact match passes
    raw = loader.read_export(spark, write_export(tmp_path, customer_table()), m, names)
    df, _ = loader.validate_and_convert(raw, m, "2026-09-25", "rel-1")
    loader.overwrite(spark, m, df, 2026092500, '{"tag":"x"}')
    loader.overwrite(spark, m, df, 2026092500, '{"tag":"x"}')   # overwrite, not append
    assert spark.table(contract.fq_target(m)).count() == 2
    for k in (loader.TXN_APP_ID, loader.TXN_VERSION, loader.USER_METADATA):
        assert spark.conf.get(k, None) is None                 # session settings cleaned up
    assert not [t for t in spark.catalog.listTables() if t.name.startswith("__bridge_src")]


def test_overwrite_replaces_all_partitions_even_with_dynamic_cluster_setting(spark):
    m = {"mapping_id": "partition_it", "target_catalog": "spark_catalog",
         "target_schema": "default", "target_table": "partition_it",
         "columns": [{"name": "id", "sql_type": "int"}]}
    target = contract.fq_target(m)
    spark.sql(f"CREATE TABLE {target} (id INT, `_bridge_release_id` STRING, "
              "`_bridge_business_date` DATE) USING parquet PARTITIONED BY (`_bridge_business_date`)")
    spark.sql(f"INSERT INTO {target} VALUES (1, 'old', DATE '2026-09-24')")
    df = spark.sql("SELECT 2 AS id, 'new' AS `_bridge_release_id`, "
                   "DATE '2026-09-25' AS `_bridge_business_date`")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        loader.overwrite(spark, m, df, 2026092500, '{"tag":"x"}')
        assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"
        assert [(r["id"], str(r["_bridge_business_date"])) for r in spark.table(target).collect()] == \
               [(2, "2026-09-25")]
    finally:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")


def test_target_schema_mismatch_is_reported(spark, inventory):
    m = dict(inventory["mappings"][0], target_catalog="spark_catalog", target_schema="default",
             target_table="customer_bad")
    spark.sql(f"CREATE TABLE {contract.fq_target(m)} (customer_id bigint, extra string) USING parquet")
    with pytest.raises(contract.ContractError) as e:
        loader.check_target_schema(spark, m)
    assert e.value.status == "FAILED_TARGET_SCHEMA" and "extra=['extra']" in str(e.value)
    missing = dict(m, target_table="does_not_exist")
    with pytest.raises(contract.ContractError, match="does not exist"):
        loader.check_target_schema(spark, missing)


def test_line_breaks_pipes_quotes_and_unicode_round_trip_exactly(spark, tmp_path):
    # Parquet stores strings length-prefixed, so delimiter characters cannot split rows or columns.
    values = ["line1\nline2", "crlf\r\nend", "a|b|c", 'say "hi"', "tab\there", "nul\x00byte",
              "emoji 😀 and 中文", "", "   ", "back\\slash", "comma,semi;colon", "\n\n|\n", None]
    table = pa.table({"id": pa.array(range(len(values)), pa.int32()), "txt": pa.array(values, pa.string())})
    m = {"columns": [{"name": "id", "sql_type": "int"}, {"name": "txt", "sql_type": "nvarchar", "max_length": 400}]}
    raw = loader.read_export(spark, write_export(tmp_path, table), m, names)
    df, rows = loader.validate_and_convert(raw, m, "2026-09-25", "r")
    assert rows == len(values)
    assert [r["txt"] for r in df.orderBy("id").collect()] == values


def test_manifest_read_like_the_plan_notebook(spark, tmp_path, inventory):
    # 00_plan reads the exact bytes (binaryFile) and parses them with csv. Reading with the
    # text source instead split the lines and dropped the BOM: this test caught that.
    from bridge import manifest
    p = tmp_path / "row_counts.csv"
    p.write_bytes(("\ufeffbusiness_date,upstream_run_id,upstream_sequence,schema,table,row_count\r\n"
                   "2026-09-25,UP1,2026092500,dbo,Customer,1204331\r\n"
                   "2026-09-25,UP1,2026092500,dbo,SalesOrder,58412907\r\n").encode("utf-8"))
    text = loader.read_small_text_file(spark, str(p))
    mf = manifest.parse(text, inventory, inventory["mappings"], expected_business_date="2026-09-25",
                        require_run_id=True)
    assert sorted(mf.counts.values()) == [1204331, 58412907] and mf.upstream_run_id == "UP1"


# v2.4 regression, moved here in v2.5 so Spark-free test modules collect without PySpark.
def test_missing_folder_and_missing_jdbc_success_marker_are_errors(spark, tmp_path, inventory):
    m = inventory["mappings"][0]
    def absent(path):
        raise FileNotFoundError(path)
    with pytest.raises(FileNotFoundError):
        loader.read_export(spark, str(tmp_path / "missing"), m, absent)
    d = tmp_path / "complete-jdbc"
    d.mkdir()
    with pytest.raises(RuntimeError, match="_SUCCESS"):
        loader.read_export(spark, str(d), m, lambda path: [], require_success=True)
    assert loader.read_export(spark, str(d), m, lambda path: ["_SUCCESS"],
                              require_success=True).count() == 0


def test_history_projection_renders_commit_time_in_utc(spark):
    # The shape of DESCRIBE HISTORY (the columns the bridge reads); Delta itself is
    # not available locally.
    df = spark.sql("SELECT CAST(7 AS BIGINT) AS version, TIMESTAMP'2026-09-25 20:59:58.123456' AS timestamp, "
                   "'WRITE' AS operation, map('mode', 'Overwrite') AS operationParameters, "
                   "CAST(NULL AS STRING) AS userMetadata, map('numOutputRows', '10') AS operationMetrics")
    row = loader.project_history(df).first()
    assert row["timestamp_utc"] == "2026-09-25T20:59:58.123456"
    hist = [{"version": int(row["version"]), "timestamp_utc": row["timestamp_utc"]}]
    assert planning.commit_utc(hist, 7) == dt.datetime(2026, 9, 25, 20, 59, 58, 123456)


def test_hidden_marker_name_is_refused_not_reported_missing(spark, tmp_path):
    """Spark ignores '_SUCCESS' even as an explicit path; that must not look like 'missing'."""
    p = tmp_path / "_SUCCESS"
    p.write_text('{"business_date":"2026-09-25"}')
    assert spark.read.format("binaryFile").load(str(p)).first() is None   # the underlying Spark behaviour
    with pytest.raises(ValueError, match="hidden file names"):
        loader.read_small_text_file(spark, str(p))


def test_governance_exemptions_and_runtime_floor(inventory):
    """v2.6.2: reviewed ABAC exemptions, table-level policies never exempt, runtime 17+ required."""
    import json
    m = inventory["mappings"][0]

    class Result:
        def __init__(self, rows): self.rows = rows
        def collect(self): return self.rows
        def first(self): return self.rows[0] if self.rows else None

    class Conf:
        def __init__(self, version): self.version = version
        def get(self, key, default=None):
            return self.version if key == loader.RUNTIME_VERSION_CONF else default

    class Spark:
        def __init__(self, policy=(), direct=None, version="18.x-scala2.13"):
            self.policy, self.direct, self.conf = list(policy), direct or {}, Conf(version)
        def sql(self, statement):
            if statement.startswith("SHOW EFFECTIVE POLICIES"):
                return Result(self.policy)
            return Result([(json.dumps({"columns": [{"name": "id"}], **self.direct}),)])

    inherited = [{"Policy Type": "COLUMN_MASK", "Policy Name": "catalog_pii_mask", "Catalog": "analytics",
                  "Schema": None, "Table": None}]
    settings = {"governance": {"exempt_abac_policies": [{"catalog": "analytics", "name": "catalog_pii_mask"}]}}
    # An inherited catalog-level mask blocks every target until it is reviewed...
    with pytest.raises(contract.ContractError, match="exempt_abac_policies"):
        loader.check_target_governance(Spark(policy=inherited), m, {})
    # ...and a reviewed exemption lets the target through.
    assert loader.check_target_governance(Spark(policy=inherited), m, settings) == \
        ["COLUMN_MASK:analytics/catalog_pii_mask"]
    # An exemption never covers a second, unreviewed policy or a table-level mask.
    with pytest.raises(contract.ContractError, match="schema_filter"):
        loader.check_target_governance(Spark(policy=inherited + [{"Policy Type": "ROW_FILTER",
                                                                   "Policy Name": "schema_filter",
                                                                   "Catalog": "analytics", "Schema": "gold"}]), m, settings)
    with pytest.raises(contract.ContractError, match="table-level column_masks"):
        loader.check_target_governance(Spark(policy=inherited, direct={"column_masks": [{"column_name": "x"}]}),
                                       m, settings)
    # GRANT/DENY policy types are not row filters or masks.
    loader.check_target_governance(Spark(policy=[{"Policy Type": "GRANT", "Policy Name": "g"}]), m, {})
    # Runtime floor: the JSON keys do not exist before 17.0, so fail closed there.
    with pytest.raises(contract.ContractError, match="Runtime 17"):
        loader.check_target_governance(Spark(version="16.4.x-scala2.13"), m, {})
    loader.check_target_governance(Spark(version="17.3.x-scala2.13"), m, {})


def test_acceptance_notebook_mappings_publish_through_the_loader(spark):
    """v2.6.2: evaluate the acceptance notebook's own mapping literals and publish with them.

    v2.6.1 shipped mappings without mapping_id; loader.overwrite raised KeyError
    on the first acceptance write, so the go-live gate could never pass.
    """
    import ast
    from pathlib import Path
    source = (Path(__file__).resolve().parents[1] / "src" / "notebooks" / "acceptance_checks.py").read_text()
    literals = {}
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in ("scratch_mapping", "partition_mapping"):
                    literals[target.id] = eval(compile(ast.Expression(node.value), "<acceptance>", "eval"),
                                               {"cat": "spark_catalog", "sch": "default"})
    assert set(literals) == {"scratch_mapping", "partition_mapping"}
    for name, mapping in literals.items():
        mapping = dict(mapping, target_table=f"acc_{name}")
        cols = ", ".join(f"`{n}` {t}" for n, t in contract.target_schema(mapping).items())
        spark.sql(f"DROP TABLE IF EXISTS {contract.fq_target(mapping)}")
        spark.sql(f"CREATE TABLE {contract.fq_target(mapping)} ({cols}) USING parquet")
        loader.check_target_schema(spark, mapping)
        data = spark.range(2).selectExpr(*[f"CAST(id AS INT) AS `{c['name']}`" for c in mapping["columns"]],
                                         "DATE '2026-01-01' AS _bridge_business_date",
                                         "'acceptance' AS _bridge_release_id")
        loader.overwrite(spark, mapping, data, 10,
                         planning.user_metadata("r10", 10, "2026-01-01", mapping["mapping_id"], "0", "a"))
        assert spark.table(contract.fq_target(mapping)).count() == 2, name


def test_fence_released_reads_finalize_events_for_this_job_run_only(spark):
    from bridge import control
    spark.sql("CREATE DATABASE IF NOT EXISTS ctl_fence")
    settings = {"control": {"catalog": "spark_catalog", "schema": "ctl_fence"}}
    control.log_run(spark, settings, release_id="r", job_run_id="42", event="PLANNED")
    assert control.fence_released(spark, settings, "42") is False
    control.log_run(spark, settings, release_id="r", job_run_id="41", event="FINISHED")
    assert control.fence_released(spark, settings, "42") is False      # another job run
    control.log_run(spark, settings, release_id="r", job_run_id="42", event="FINISHED_WITH_FAILURES")
    assert control.fence_released(spark, settings, "42") is True
    assert control.fence_released(spark, settings, "41") is True
    with pytest.raises(ValueError):
        control.fence_released(spark, settings, "42' OR '1'='1")
