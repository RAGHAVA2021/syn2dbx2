"""Spark side: validate the exported Parquet, convert types, overwrite the target.

Nothing here writes to the target until every check has passed. The single
write is an INSERT OVERWRITE into the existing table (not a drop/recreate), so
grants, row filters, column masks, comments and history stay attached and
readers see either the previous night or the new one, never a mix.
"""
from pyspark.sql import functions as F

from . import contract, manifest, planning


AncientCetasTimestamp = contract.AncientCetasTimestamp

TXN_APP_ID = "spark.databricks.delta.write.txnAppId"
TXN_VERSION = "spark.databricks.delta.write.txnVersion"
USER_METADATA = "spark.databricks.delta.commitInfo.userMetadata"


def has_data_files(entries):
    """entries: names in the export folder. Hidden and _-prefixed files are not data."""
    return any(not (n.startswith("_") or n.startswith(".")) for n in entries)


def read_export(spark, path, m, list_names, require_success=False):
    """Read an existing stage; an existing folder without data files is empty."""
    names = list_names(path)  # a missing folder must raise, never become an empty export
    if require_success and "_SUCCESS" not in names:
        raise RuntimeError(f"JDBC stage has no _SUCCESS marker: {path}")
    if has_data_files(names):
        return spark.read.parquet(path)
    # A zero-row projection produces the exact contract schema without
    # materializing data (portable across Spark 3.5 and 4.x).
    return spark.range(0).selectExpr(*[
        f"CAST(NULL AS {c['parquet_type']}) AS {contract.q_spark(c['name'])}"
        for c in contract.columns(m)])




def _uc_jdbc_reader(spark, connection_name, timeout=None):
    if not connection_name or not isinstance(connection_name, str):
        raise ValueError("synapse.uc_jdbc_connection is required for serverless JDBC fallback")
    reader = (spark.read.format("jdbc")
              .option("databricks.connection", connection_name)
              # Wall-clock datetimes independent of the JVM time zone (invariant 9).
              .option("preferTimestampNTZ", "true"))
    if timeout:
        reader = reader.option("queryTimeout", str(int(timeout)))
    return reader


def uc_jdbc_export(spark, connection_name, query, target_path, *, timeout=None, fetch_size=None,
                   partition_column=None, bounds=None, num_partitions=1):
    """Serverless fallback through a Unity Catalog JDBC connection.

    Only the governed connection name and native SELECT are supplied at query time.
    URL, driver and credentials remain inside the Unity Catalog connection.
    The UC connection must allow the read options stage_jdbc_query sets (the
    acceptance job proves it).
    """
    stage_jdbc_query(_uc_jdbc_reader(spark, connection_name, timeout), query, target_path,
                     fetch_size=fetch_size, partition_column=partition_column, bounds=bounds,
                     num_partitions=num_partitions)


def stage_jdbc_query(reader, query, target_path, *, fetch_size=None, partition_column=None, bounds=None,
                     num_partitions=1):
    """Read `query` with a configured JDBC reader and stage it once as Parquet at target_path.

    Staging to Parquet (instead of loading the JDBC DataFrame straight into Delta)
    means the slow source is read exactly once, and everything after this point is
    identical to the CETAS path: same Parquet validation, count check and overwrite.
    Reads in parallel when a partition column and bounds are given. Never
    overwrites an existing folder.
    """
    if fetch_size:
        reader = reader.option("fetchsize", str(int(fetch_size)))
    if bounds and partition_column and int(num_partitions) > 1:
        # Spark refuses "query" together with partitionColumn: use a derived table.
        reader = (reader.option("dbtable", f"({query}) AS bridge_src")
                  .option("partitionColumn", partition_column)
                  .option("lowerBound", bounds[0]).option("upperBound", bounds[1])
                  .option("numPartitions", str(int(num_partitions))))
    else:
        reader = reader.option("query", query)
    reader.load().write.mode("errorifexists").parquet(target_path)


def uc_jdbc_row(spark, connection_name, query, timeout=None):
    """First row of a small query through the governed connection, as a tuple (or None)."""
    row = _uc_jdbc_reader(spark, connection_name, timeout).option("query", query).load().first()
    return None if row is None else tuple(row)


def uc_jdbc_count(spark, connection_name, query, timeout=None):
    """Run a scalar COUNT query through the governed Unity Catalog JDBC connection."""
    row = uc_jdbc_row(spark, connection_name, query, timeout)
    if row is None or len(row) < 1 or row[0] is None:
        raise RuntimeError("UC-JDBC count query returned no value")
    return int(row[0])


MAX_MANIFEST_BYTES = 20 * 1024 * 1024


# Pure (Spark-free) so manifest logic and its tests do not need PySpark.
is_missing_path_error = manifest.is_missing_path_error


def read_small_text_file(spark, path):
    """Exact text of a small file (the upstream manifest).

    Uses the binaryFile source so the bytes arrive untouched. The text source
    splits lines (ignoring wholetext when set as an option) and drops a BOM, which
    would make a valid manifest look empty. Decoded as UTF-8, BOM tolerated.
    """
    name = path.rstrip("/").rsplit("/", 1)[-1]
    if name.startswith(("_", ".")):
        # Spark file sources silently ignore "_"/"." names even when the exact
        # file is given ("All paths were ignored"), which would look like a
        # missing file. Callers must read such files with dbutils.fs.head.
        raise ValueError(f"{path}: Spark cannot read hidden file names; use dbutils.fs.head")
    row = spark.read.format("binaryFile").load(path).select("content", "length").first()
    if row is None:
        raise FileNotFoundError(path)
    if row["length"] > MAX_MANIFEST_BYTES:
        raise ValueError(f"{path} is {row['length']} bytes; a row-count manifest should be far smaller")
    return bytes(row["content"]).decode("utf-8-sig")


def check_parquet_schema(df, m):
    cols = contract.columns(m)
    actual = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    expected = [c["name"] for c in cols]
    if [a[0] for a in actual] != expected:
        raise contract.ContractError("FAILED_CONTRACT", f"Parquet columns {[a[0] for a in actual]} != {expected}")
    for (name, typ), c in zip(actual, cols):
        ok = contract.PARQUET_ACCEPT.get(c["parquet_type"], {c["parquet_type"]})
        if typ not in ok:
            raise contract.ContractError("FAILED_CONTRACT", f"{name}: Parquet type {typ}, expected {c['parquet_type']}")


def _convert_expr(c):
    return f"try_cast({contract.q_spark(c['name'])} AS {c['target_type']})"


def validate_and_convert(df, m, business_date, release, reject_ancient_timestamps=False):
    """One pass over the data: row count plus per-column conversion-loss counts.

    try_cast is used so that a lossy conversion is counted instead of throwing
    under ANSI mode (the serverless and recent-runtime default). Any loss is a
    deterministic contract failure.
    """
    check_parquet_schema(df, m)
    cols = contract.columns(m)
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for i, c in enumerate(cols):
        lost = F.col(c["name"]).isNotNull() & F.expr(_convert_expr(c)).isNull()
        aggs.append(F.sum(F.when(lost, 1).otherwise(0)).alias(f"__lost_{i}"))
        if reject_ancient_timestamps and c["target_type"] in ("timestamp", "timestamp_ntz"):
            cutoff = F.lit("1900-01-01 00:00:00").cast(c["target_type"])
            aggs.append(F.sum(F.when(F.col(c["name"]) < cutoff, 1).otherwise(0)).alias(f"__ancient_{i}"))
    stats = df.agg(*aggs).first()
    if reject_ancient_timestamps and any(stats[f"__ancient_{i}"] for i, c in enumerate(cols)
                                        if c["target_type"] in ("timestamp", "timestamp_ntz")):
        raise AncientCetasTimestamp("FAILED_CONTRACT", "CETAS contains timestamps before 1900; "
                                    "INT96 may shift historical times; use the Spark-written JDBC fallback")
    lost = {c["name"]: int(stats[f"__lost_{i}"] or 0) for i, c in enumerate(cols) if stats[f"__lost_{i}"]}
    if lost:
        raise contract.ContractError("FAILED_CONTRACT", f"conversion would lose non-null values: {lost}")
    converted = df.select(*[F.expr(_convert_expr(c)).alias(c["name"]) for c in cols],
                          F.lit(str(business_date)).cast("date").alias("_bridge_business_date"),
                          F.lit(release).alias("_bridge_release_id"))
    return converted, int(stats["__rows"])


def check_target_schema(spark, m):
    target = contract.fq_target(m)
    if not spark.catalog.tableExists(target.replace("`", "")):
        raise contract.ContractError("FAILED_TARGET_SCHEMA", f"target {target} does not exist; run the UC setup script")
    actual = {f.name.lower(): f.dataType.simpleString() for f in spark.table(target).schema.fields}
    expected = contract.target_schema(m)
    if actual != expected:
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        wrong = sorted(k for k in set(actual) & set(expected) if actual[k] != expected[k])
        raise contract.ContractError("FAILED_TARGET_SCHEMA",
                                     f"target {target} differs: missing={missing} extra={extra} type={wrong}")
    properties = {str(r["key"]).lower(): r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    if "delta.settransactionretentionduration" in properties:
        raise contract.ContractError("FAILED_TARGET_SCHEMA",
                                     f"target {target} expires Delta transaction IDs; remove "
                                     "delta.setTransactionRetentionDuration after reviewing other writers")


RUNTIME_VERSION_CONF = "spark.databricks.clusterUsageTags.sparkVersion"


def check_runtime_for_governance(spark):
    """The JSON row_filter/column_masks keys exist only from Runtime 17.0.

    On an older cluster the governance check would pass silently (fail open),
    so refuse it. Outside Databricks (local tests) the tag is absent.
    """
    version = spark.conf.get(RUNTIME_VERSION_CONF, None)
    if version is None:
        return
    major = contract.runtime_major(version)
    if major is None or major < contract.MIN_GOVERNANCE_RUNTIME_MAJOR:
        raise contract.ContractError(
            "FAILED_TARGET_SCHEMA", f"runtime {version!r} cannot expose table-level row filters/masks in "
            f"DESCRIBE ... AS JSON; use Databricks Runtime {contract.MIN_GOVERNANCE_RUNTIME_MAJOR}+ (18 is pinned)")


def check_target_governance(spark, m, settings=None):
    """Fail before extraction when a target has policies this overwrite cannot serve.

    SHOW EFFECTIVE POLICIES covers inherited ABAC. DESCRIBE EXTENDED AS JSON
    inspects current table-level filters and masks directly; information_schema
    may lag some metadata changes. Metadata failures are fatal. ABAC policies
    reviewed under governance.exempt_abac_policies (the job and rollback
    identities are in their EXCEPT clause) do not block; see
    contract.governance_problems.
    """
    target = contract.fq_target(m)
    check_runtime_for_governance(spark)
    try:
        effective = [r.asDict() if hasattr(r, "asDict") else dict(r)
                     for r in spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE {target}").collect()]
        described = spark.sql(f"DESCRIBE TABLE EXTENDED {target} AS JSON").first()
        if described is None:
            raise ValueError("empty DESCRIBE result")
        blocking, exempted = contract.governance_problems(
            effective, str(described[0]), contract.exempt_abac_policies(settings or {}))
    except Exception as exc:
        raise contract.ContractError("FAILED_TARGET_SCHEMA",
                                     f"cannot inspect governance of {target}: {exc}") from exc
    if blocking:
        raise contract.ContractError("FAILED_TARGET_SCHEMA",
                                     f"target {target} has governed policies {blocking}; dedicated "
                                     "INSERT OVERWRITE/RESTORE is not qualified. An ABAC policy whose EXCEPT "
                                     "clause lists the job and rollback identities can be added to "
                                     "governance.exempt_abac_policies after review, as "
                                     "{catalog, schema?, table?, name} of the securable it is defined on")
    return exempted


def project_history(df):
    """The DESCRIBE HISTORY columns the bridge uses, plus the commit time as text.

    timestamp_utc is rendered in the session time zone (UTC, set by the
    bootstrap), so it never depends on the Python driver's local zone.
    """
    return df.select("version", "operation", "userMetadata", "operationParameters", "operationMetrics",
                     F.date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias("timestamp_utc"))


def _history_rows(spark, sql):
    return project_history(spark.sql(sql)).collect()


def history(spark, m, limit=200):
    rows = _history_rows(spark, f"DESCRIBE HISTORY {contract.fq_target(m)} LIMIT {int(limit)}")
    if len(rows) == int(limit) and not any(planning.parse_metadata(r["userMetadata"]) for r in rows):
        # Maintenance may fill the bounded window. Searching the complete
        # retained history avoids mistaking a missing bridge commit for a new table.
        rows = _history_rows(spark, f"DESCRIBE HISTORY {contract.fq_target(m)}")
    return [{"version": int(r["version"]), "operation": r["operation"], "userMetadata": r["userMetadata"],
             "operationParameters": dict(r["operationParameters"] or {}),
             "operationMetrics": dict(r["operationMetrics"] or {}),
             "timestamp_utc": r["timestamp_utc"]} for r in rows]


def overwrite(spark, m, converted, sequence, metadata):
    """INSERT OVERWRITE guarded by Delta's idempotent-write identifiers.

    With txnAppId constant per table and txnVersion = upstream sequence, Delta
    skips the write when a commit with the same or a higher version already
    exists for that app id: a retry after a lost acknowledgement is a no-op and
    an older night can never overwrite a newer one. A skipped write returns
    normally, so the caller must classify the outcome from history.

    The settings are session-scoped; each notebook task has its own session and
    processes one table at a time, so they cannot leak between tables.
    """
    view = f"__bridge_src_{m['mapping_id'].replace('-', '_')}"
    converted.createOrReplaceTempView(view)
    cols = ", ".join(contract.q_spark(c["name"]) for c in contract.columns(m))
    cols += ", `_bridge_business_date`, `_bridge_release_id`"
    spark.conf.set(TXN_APP_ID, planning.txn_app_id(m["mapping_id"]))
    spark.conf.set(TXN_VERSION, str(int(sequence)))
    spark.conf.set(USER_METADATA, metadata)
    # A cluster-level dynamic partition setting would otherwise leave untouched
    # partitions behind on a full refresh of a partitioned target.
    partition_mode = "spark.sql.sources.partitionOverwriteMode"
    old_partition_mode = spark.conf.get(partition_mode, None)
    spark.conf.set(partition_mode, "static")
    try:
        spark.sql(f"INSERT OVERWRITE TABLE {contract.fq_target(m)} ({cols}) SELECT {cols} FROM `{view}`")
    finally:
        if old_partition_mode is None:
            spark.conf.unset(partition_mode)
        else:
            spark.conf.set(partition_mode, old_partition_mode)
        for k in (TXN_APP_ID, TXN_VERSION, USER_METADATA):
            spark.conf.unset(k)
        spark.catalog.dropTempView(view)


def restore(spark, m, version, metadata):
    spark.conf.set(USER_METADATA, metadata)
    try:
        spark.sql(f"RESTORE TABLE {contract.fq_target(m)} TO VERSION AS OF {int(version)}")
    finally:
        spark.conf.unset(USER_METADATA)
