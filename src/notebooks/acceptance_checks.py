# Databricks notebook source
# MAGIC %md
# MAGIC # Acceptance checks — run once per workspace before go-live
# MAGIC Proves on your runtime the Delta behaviour this design depends on. It uses a
# MAGIC scratch table in `scratch_schema` and drops it at the end.
# MAGIC
# MAGIC 1. `INSERT OVERWRITE` honours the SQL idempotent-write settings
# MAGIC    (`txnAppId`/`txnVersion`): same version again is skipped, a lower version is
# MAGIC    skipped, a higher version is applied.
# MAGIC 2. `userMetadata` and `numOutputRows` appear in `DESCRIBE HISTORY`.
# MAGIC 3. `INSERT OVERWRITE` keeps an actual SELECT grant and table properties.
# MAGIC 4. `RESTORE` works and does not look like a published night.
# MAGIC 5. A later untagged data write invalidates an old publication.
# MAGIC 6. Empty and partitioned overwrites replace all rows and expose metrics.
# MAGIC 7. An actual OPTIMIZE history entry does not invalidate a publication.
# MAGIC 8. `loader.history` exposes each commit's time in UTC (the deadline check uses it).
# MAGIC 9. Writes go through the production `loader.overwrite`, and a static full overwrite is
# MAGIC    not recorded as `isDynamicPartitionOverwrite` even with a dynamic session default.
# MAGIC 10. The runtime is 17+ (when the tag is exposed) and the governance preflight
# MAGIC    (`SHOW EFFECTIVE POLICIES`, `DESCRIBE ... AS JSON`) runs on a plain target and
# MAGIC    blocks the same target once a row filter or a column mask is applied.
# MAGIC 11. A missing staging folder and a missing manifest/marker are classified as missing,
# MAGIC    not as errors (the plan's wait loop and stage reuse depend on this).
# MAGIC 12. Every Spark conf the nightly tasks set is accepted by this compute.
# MAGIC 13. The UC JDBC connection is usable by the job identity, keeps datetime2 wall-clock
# MAGIC    values (including pre-1900) and runs the fallback COUNT_BIG query, through the
# MAGIC    production `loader.uc_jdbc_export` with every read option the nightly passes
# MAGIC    (queryTimeout, fetchsize, preferTimestampNTZ and a partitioned read).
# MAGIC 14. The serverless CETAS command path: `mssql-python` is installed by the job
# MAGIC    environment, the job identity logs in to the pool, the reused staging objects pass
# MAGIC    the plan's check, and a one-row CETAS lands Parquet that Spark can read.
# MAGIC
# MAGIC Rerun this job after serverless release notes list behavioural changes.
# MAGIC
# MAGIC Every check raises on failure. The job runs on serverless, like the nightly job.
# MAGIC A plain scratch table does not prove writes on targets with row filters or
# MAGIC masks; those need a separate protected-table trial before go-live.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import uuid

from bridge import manifest

settings = load_settings()
runtime = spark.conf.get(loader.RUNTIME_VERSION_CONF, None)
print(f"runtime: {runtime}")
if runtime is None:
    # Serverless may not expose the runtime tag. The governance preflight then cannot
    # check the version, so the row-filter / column-mask probe below must prove that
    # policies are detected on this compute.
    print("runtime tag not exposed (serverless): relying on the governance detection probe")
elif contract.runtime_major(runtime) is None or contract.runtime_major(runtime) < contract.MIN_GOVERNANCE_RUNTIME_MAJOR:
    raise AssertionError(f"ACCEPTANCE FAILED: runtime {runtime!r} is below Databricks Runtime "
                         f"{contract.MIN_GOVERNANCE_RUNTIME_MAJOR}")

# Every Spark conf the nightly tasks set. Serverless accepts only a limited set;
# an unsupported one fails the bootstrap or, worse, the idempotent overwrite.
_NIGHTLY_CONFS = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
    "spark.sql.sources.partitionOverwriteMode": "static",
    loader.TXN_APP_ID: "bridge:acceptance-probe",
    loader.TXN_VERSION: "1",
    loader.USER_METADATA: "{}",
}
for _key, _value in _NIGHTLY_CONFS.items():
    _prior = None
    try:
        _prior = spark.conf.get(_key, None)
        spark.conf.set(_key, _value)
        if spark.conf.get(_key, None) != _value:
            raise RuntimeError(f"read back {spark.conf.get(_key, None)!r}")
    except Exception as e:  # noqa: BLE001 - reported as an acceptance failure
        raise AssertionError(f"ACCEPTANCE FAILED: this compute rejects Spark conf {_key}: {e}") from e
    finally:
        try:
            spark.conf.unset(_key) if _prior is None else spark.conf.set(_key, _prior)
        except Exception:  # noqa: BLE001
            pass
print("ok - every Spark conf the nightly job sets is accepted on this compute")
scratch = widget("scratch_schema")  # catalog.schema the job identity may create tables in
grant_principal = widget("grant_principal")
if not grant_principal or "`" in grant_principal or "\n" in grant_principal:
    raise ValueError("grant_principal must be an existing group with no backticks or newlines")
if spark.conf.get("spark.sql.parquet.datetimeRebaseModeInRead") != "CORRECTED" or \
        spark.conf.get("spark.sql.parquet.int96RebaseModeInRead") != "CORRECTED":
    raise AssertionError("Parquet calendar rebase settings must be CORRECTED")
cat, sch = scratch.split(".")
t = f"{contract.q_spark(cat)}.{contract.q_spark(sch)}.`bridge_acceptance_txn`"
spark.sql(f"DROP TABLE IF EXISTS {t}")
spark.sql(f"CREATE TABLE {t} (id INT, _bridge_business_date DATE, _bridge_release_id STRING) TBLPROPERTIES "
          "('delta.deletedFileRetentionDuration'='interval 7 days', "
          "'delta.autoOptimize.autoCompact'='false')")
spark.sql(f"GRANT SELECT ON TABLE {t} TO `{grant_principal}`")
def select_grant():
    return any(str(r["principal"]) == grant_principal and str(r["actionType"]).upper() == "SELECT"
               for r in spark.sql(f"SHOW GRANTS ON TABLE {t}").collect())
assert select_grant(), "scratch SELECT grant did not take effect"
props_before = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {t}").collect()}
scratch_mapping = {"mapping_id": "acceptance", "target_catalog": cat, "target_schema": sch,
                   "target_table": "bridge_acceptance_txn", "columns": [{"name": "id", "sql_type": "int"}]}


def write(version, n, release, many_files=False):
    df = spark.range(n).selectExpr("CAST(id AS INT) AS id",
        "DATE '2026-01-01' AS _bridge_business_date", "'acceptance' AS _bridge_release_id")
    if many_files:
        df = df.repartition(8)
    loader.overwrite(spark, scratch_mapping, df, version,
        planning.user_metadata(release, version, "2026-01-01", "acceptance", "0", "a"))


def hist():
    return [{"version": int(r["version"]), "operation": r["operation"], "userMetadata": r["userMetadata"],
             "operationParameters": dict(r["operationParameters"] or {}),
             "operationMetrics": dict(r["operationMetrics"] or {})}
            for r in spark.sql(f"DESCRIBE HISTORY {t}").collect()]


def check(cond, msg):
    if not cond:
        raise AssertionError("ACCEPTANCE FAILED: " + msg)
    print("ok -", msg)


write(10, 5, "r10")
check(spark.table(t).count() == 5, "first overwrite applied")
v1 = hist()[0]["version"]
check(planning.verify_after_write(hist(), "r10", 10, 5)[0] == "PUBLISHED", "history has userMetadata + numOutputRows")
committed = planning.commit_utc(loader.history(spark, scratch_mapping, 50), v1)
check(committed is not None and abs((planning.utcnow().replace(tzinfo=None) - committed).total_seconds()) < 3600,
      "loader.history exposes the commit time in UTC (used by the finalize deadline check)")

write(10, 7, "r10-retry")
check(hist()[0]["version"] == v1 and spark.table(t).count() == 5, "same txnVersion is skipped (retry is a no-op)")

write(9, 3, "r9")
check(hist()[0]["version"] == v1 and spark.table(t).count() == 5, "lower txnVersion is skipped (old night blocked)")
check(planning.verify_after_write(hist(), "r9", 9, 3)[0] == "SUPERSEDED", "skipped old write classified SUPERSEDED")

write(11, 8, "r11", many_files=True)
check(spark.table(t).count() == 8, "higher txnVersion is applied")
spark.sql(f"OPTIMIZE {t}")
check(hist()[0]["operation"] == "OPTIMIZE" and planning.precheck(hist(), 11, "2026-01-01") == "ALREADY_CURRENT",
      "OPTIMIZE history does not invalidate the published version")

props_after = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {t}").collect()}
check(props_after.get("delta.deletedFileRetentionDuration") == props_before.get("delta.deletedFileRetentionDuration"),
      "INSERT OVERWRITE keeps table properties")
check(select_grant(), "INSERT OVERWRITE keeps SELECT grant")

spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('bridge.acceptance'='checked')")
check(planning.precheck(hist(), 11, "2026-01-01") == "ALREADY_CURRENT",
      "SET TBLPROPERTIES does not invalidate a verified data publication")
spark.sql(f"ALTER TABLE {t} UNSET TBLPROPERTIES ('bridge.acceptance')")
check(planning.precheck(hist(), 11, "2026-01-01") == "ALREADY_CURRENT",
      "UNSET TBLPROPERTIES does not invalidate a verified data publication")
spark.sql(f"VACUUM {t}")
check(planning.precheck(hist(), 11, "2026-01-01") == "ALREADY_CURRENT",
      "the runtime's VACUUM operations do not invalidate a verified publication")

spark.sql(f"INSERT OVERWRITE TABLE {t} (id) SELECT id FROM range(2)")
check(planning.precheck(hist(), 11) == "FAILED_VERIFY",
      "an untagged overwrite cannot be reported as an already current release")

restore_md = planning.restore_metadata("r11", v1 + 1, v1, "acceptance")
loader.restore(spark, {"target_catalog": cat, "target_schema": sch, "target_table": "bridge_acceptance_txn"},
               v1, restore_md)
check(spark.table(t).count() == 5, "RESTORE works")
check(hist()[0]["operation"] == "RESTORE" and hist()[0]["userMetadata"] == restore_md,
      "RESTORE history has the metadata required by automatic verification")
check(planning.latest_bridge_commit(hist())[1]["release_id"] == "r11", "restore commit is not mistaken for a publish")
restored_version = hist()[0]["version"]
write(11, 3, "r11-retry-after-restore")
check(hist()[0]["version"] == restored_version and spark.table(t).count() == 5,
      "a RESTORE does not erase the idempotency guard for the old txnVersion")
write(12, 4, "r12-after-restore")
check(spark.table(t).count() == 4, "a higher txnVersion can publish after RESTORE")
write(13, 0, "r13-empty")
check(spark.table(t).count() == 0 and planning.verify_after_write(hist(), "r13-empty", 13, 0)[0] == "PUBLISHED",
      "empty overwrite records numOutputRows=0")

p = f"{contract.q_spark(cat)}.{contract.q_spark(sch)}.`bridge_acceptance_partitioned`"
spark.sql(f"DROP TABLE IF EXISTS {p}")
spark.sql(f"CREATE TABLE {p} (id INT, part INT, _bridge_business_date DATE, "
          "_bridge_release_id STRING) USING DELTA PARTITIONED BY (part)")
spark.sql(f"INSERT INTO {p} (id, part) VALUES (1, 1), (2, 2)")
partition_mapping = {"mapping_id": "acceptance_partitioned", "target_catalog": cat, "target_schema": sch,
                     "target_table": "bridge_acceptance_partitioned",
                     "columns": [{"name": "id", "sql_type": "int"}, {"name": "part", "sql_type": "int"}]}
partition_data = spark.range(1).selectExpr("CAST(3 AS INT) AS id", "CAST(3 AS INT) AS part",
    "DATE '2026-01-01' AS _bridge_business_date", "'acceptance' AS _bridge_release_id")
prior_mode = spark.conf.get("spark.sql.sources.partitionOverwriteMode", None)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
try:
    loader.overwrite(spark, partition_mapping, partition_data, 14,
        planning.user_metadata("r14", 14, "2026-01-01", "acceptance_partitioned", "0", "a"))
    check(spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic",
          "overwrite restores the prior partition mode")
finally:
    if prior_mode is None:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")
    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", prior_mode)
check([(r['id'], r['part']) for r in spark.table(p).collect()] == [(3, 3)],
      "static partitioned overwrite removes rows in old partitions")
p_hist = loader.history(spark, partition_mapping, 50)
check(not planning.dynamic_partition_overwrite(p_hist[0]),
      "the bridge overwrite is not recorded as isDynamicPartitionOverwrite")
check(planning.verify_after_write(p_hist, "r14", 14, 1)[0] == "PUBLISHED",
      "a static partitioned publish verifies as PUBLISHED")
spark.sql(f"DROP TABLE {p}")

# ---- governance preflight on a plain target -------------------------------------------
loader.check_target_schema(spark, scratch_mapping)
try:
    exempted = loader.check_target_governance(spark, scratch_mapping, settings)
except contract.ContractError as e:
    raise AssertionError(
        "ACCEPTANCE FAILED: the governance preflight blocks a plain scratch table in "
        f"{scratch}: {e}. Inherited ABAC policies would block every bridge target in the same "
        "catalog/schema. Review each with DESCRIBE POLICY; list only those whose EXCEPT clause names the "
        "job and rollback identities in governance.exempt_abac_policies, each as {catalog, schema?, table?, "
        "name} of the securable the policy is defined on") from e
print(f"ok - governance preflight runs on this runtime (reviewed exemptions applied: {exempted or 'none'})")

# The preflight must also DETECT table-level policies on this compute; on serverless
# this is the only proof, because the runtime version tag may be absent.
_fq = f"{contract.q_spark(cat)}.{contract.q_spark(sch)}"
spark.sql(f"CREATE OR REPLACE FUNCTION {_fq}.`bridge_acceptance_rf`(id INT) RETURNS BOOLEAN RETURN TRUE")
spark.sql(f"CREATE OR REPLACE FUNCTION {_fq}.`bridge_acceptance_mask`(id INT) RETURNS INT RETURN id")
try:
    for _apply, _remove, _what in (
            (f"ALTER TABLE {t} SET ROW FILTER {_fq}.`bridge_acceptance_rf` ON (id)",
             f"ALTER TABLE {t} DROP ROW FILTER", "row filter"),
            (f"ALTER TABLE {t} ALTER COLUMN id SET MASK {_fq}.`bridge_acceptance_mask`",
             f"ALTER TABLE {t} ALTER COLUMN id DROP MASK", "column mask")):
        spark.sql(_apply)
        try:
            try:
                loader.check_target_governance(spark, scratch_mapping, settings)
                detected = False
            except contract.ContractError:
                detected = True
            check(detected, f"governance preflight blocks a target with a table-level {_what}")
        finally:
            spark.sql(_remove)
finally:
    spark.sql(f"DROP FUNCTION IF EXISTS {_fq}.`bridge_acceptance_rf`")
    spark.sql(f"DROP FUNCTION IF EXISTS {_fq}.`bridge_acceptance_mask`")

# ---- missing paths are classified as missing (plan wait loop, stage reuse) ------------
def missing_error(action, path):
    try:
        action(path)
    except Exception as e:  # noqa: BLE001 - classified below
        return e
    raise AssertionError(f"ACCEPTANCE FAILED: {path} unexpectedly exists")

probe_id = uuid.uuid4().hex
st = settings["staging"]
missing_stage = f"{st['root']}/{st['prefix']}/_acceptance_missing_{probe_id}/"
check(manifest.is_missing_path_error(missing_error(dbutils.fs.ls, missing_stage)),
      "a missing staging folder is recognized as missing (dbutils.fs.ls)")
if manifest.enabled(settings):
    missing_csv = manifest.path_for(settings, "2099-12-31")
    check(manifest.is_missing_path_error(missing_error(dbutils.fs.ls, missing_csv)),
          "a missing manifest CSV is recognized as missing (dbutils.fs.ls)")
    marker = manifest.marker_path(settings, "2099-12-31")
    if marker:
        check(manifest.is_missing_path_error(missing_error(lambda x: dbutils.fs.head(x, 1024), marker)),
              "a missing completion marker is recognized as missing (dbutils.fs.head)")

# ---- serverless UC-JDBC fallback route (connection, grant, timestamps, count query) ----
connection_name = settings["synapse"].get("uc_jdbc_connection")
problems_4a = contract.variant_4a_problems(settings)
if problems_4a:
    raise AssertionError("ACCEPTANCE FAILED: " + "; ".join(problems_4a))
import datetime as _dt
probe_select = ("SELECT id, ts, old_ts FROM (SELECT 1 AS id, CAST('2020-01-02 03:04:05.123456' AS datetime2(6)) "
                "AS ts, CAST('1899-12-31 23:59:59' AS datetime2(6)) AS old_ts UNION ALL SELECT 2, "
                "CAST('2020-01-02 03:04:05.123456' AS datetime2(6)), CAST('1899-12-31 23:59:59' AS datetime2(6))) x")
for _partitioned in (False, True):
    staged = f"{st['root']}/{st['prefix']}/_acceptance_uc_jdbc_{probe_id}_{int(_partitioned)}"
    loader.uc_jdbc_export(spark, connection_name, probe_select, staged, timeout=300,
                          fetch_size=contract.execution(settings, "jdbc_fetch_size"),
                          partition_column="id" if _partitioned else None,
                          bounds=("1", "2") if _partitioned else None,
                          num_partitions=2 if _partitioned else 1)
    try:
        rows = spark.read.parquet(staged).selectExpr("id", "CAST(ts AS timestamp_ntz) AS ts",
                                                     "CAST(old_ts AS timestamp_ntz) AS old_ts").collect()
    finally:
        dbutils.fs.rm(staged, True)
    what = "partitioned " if _partitioned else ""
    check(sorted(r["id"] for r in rows) == [1, 2], f"UC-JDBC {what}fallback read returns every row")
    check(all(r["ts"] == _dt.datetime(2020, 1, 2, 3, 4, 5, 123456) for r in rows),
          f"UC-JDBC {what}fallback keeps datetime2 wall-clock values (got {rows[0]['ts']})")
    check(all(r["old_ts"] == _dt.datetime(1899, 12, 31, 23, 59, 59) for r in rows),
          f"UC-JDBC {what}fallback keeps pre-1900 values exactly (got {rows[0]['old_ts']})")
count_probe = {"source_schema": "sys", "source_object": "objects"}
check(loader.uc_jdbc_count(spark, connection_name, contract.count_sql(count_probe), 300) > 0,
      "the fallback COUNT_BIG query runs through the UC JDBC connection")
bounds_probe = {"source_schema": "sys", "source_object": "objects",
                "columns": [{"name": "object_id", "sql_type": "int"}], "jdbc_partition_column": "object_id"}
check(loader.uc_jdbc_row(spark, connection_name, contract.bounds_sql(bounds_probe), 300)[0] is not None,
      "the partition-bounds query runs through the UC JDBC connection")

# ---- serverless CETAS command path (mssql-python) --------------------------------------
get_props = synapse.credentials_provider(settings, dbutils)
check(synapse._jvm_gateway(spark) is None, "this compute has no JVM gateway, so CETAS uses mssql-python")
_cetas_probe = {"mapping_id": "acceptance", "source_schema": "sys", "source_object": "objects",
                        "columns": [{"name": "one", "sql_type": "int"}]}
_ext = process.external_table_name(_cetas_probe, process.new_attempt_id())
_rel = f"{st['prefix']}/_acceptance_cetas_{probe_id}/"
with synapse.connect(spark, settings, get_props) as _conn:
    check(_conn.query("SELECT 1", timeout=60)[0][0] == "1", "mssql-python connects to the pool as the job identity")
    synapse.check_staging_objects(_conn, settings)
    print("ok - reused staging objects pass the plan's check")
    _schema, _ds, _ff = (contract.q_sql(st["metadata_schema"]), contract.q_sql(st["external_data_source"]),
                         contract.q_sql(st["file_format"]))
    try:
        _conn.execute(f"CREATE EXTERNAL TABLE {_schema}.{contract.q_sql(_ext)} WITH (LOCATION = '{_rel}', "
                      f"DATA_SOURCE = {_ds}, FILE_FORMAT = {_ff}) AS SELECT CAST(1 AS int) AS [one]", 600)
    finally:
        _conn.execute(contract.drop_external_sql(settings, _ext), 300)
try:
    check(spark.read.parquet(f"{st['root']}/{_rel}").collect()[0]["one"] == 1,
          "a CETAS issued over mssql-python lands Parquet that Spark reads")
finally:
    dbutils.fs.rm(f"{st['root']}/{_rel}", True)

spark.sql(f"DROP TABLE {t}")
print("\nALL ACCEPTANCE CHECKS PASSED")
