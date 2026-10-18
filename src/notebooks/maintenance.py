# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance (weekly)
# MAGIC * VACUUM every target (skip this if predictive optimization is enabled on the
# MAGIC   schemas: it already vacuums UC managed tables).
# MAGIC * Prune control-log rows older than `control_retention_days`.
# MAGIC * Drop CETAS external-table definitions left behind by crashed exports
# MAGIC   (definitions only; the files are removed by the ADLS lifecycle rule).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from pyspark.sql import functions as F

settings = load_settings()
do_vacuum = widget("vacuum", "false").lower() == "true"
optimize_control = widget("optimize_control", "true").lower() == "true"
retention_days = int(widget("control_retention_days", "400"))
drop_orphans = widget("drop_orphan_external_tables", "true").lower() == "true"

if do_vacuum:
    vacuum_failures = []
    for m in settings["mappings"]:
        try:
            if not m.get("enabled", True) and not spark.catalog.tableExists(contract.fq_target(m).replace("`", "")):
                print("skipped disabled mapping without target", m["mapping_id"])
                continue
            spark.sql(f"VACUUM {contract.fq_target(m)}")   # uses the table's own retention setting
            print("vacuumed", m["mapping_id"])
        except Exception as e:
            vacuum_failures.append(f"{m['mapping_id']}: {e}")

for t, col in (("table_log", "event_utc"), ("run_log", "event_utc"), ("plan", "planned_utc")):
    spark.sql(f"DELETE FROM {contract.fq_control(settings, t)} "
              f"WHERE {col} < current_timestamp() - INTERVAL {int(retention_days)} DAYS")
    if optimize_control:
        spark.sql(f"OPTIMIZE {contract.fq_control(settings, t)}")

if drop_orphans:
    schema = settings["staging"]["metadata_schema"]
    # Older than 2x the export timeout, so no running CETAS can still own one.
    hours = max(6, 2 * settings["execution"]["export_timeout_seconds"] // 3600 + 1)
    with synapse.connect(spark, settings, synapse.credentials_provider(settings, dbutils)) as conn:
        names = [r[0] for r in conn.query(
            "SELECT t.name FROM sys.external_tables t JOIN sys.schemas s ON s.schema_id=t.schema_id "
            "WHERE s.name=? AND t.name LIKE 'x[_]%' AND t.create_date < DATEADD(hour, -CAST(? AS int), GETDATE())",
            schema, hours)]
        # The Synapse schema may be shared. A name beginning with x_ is not
        # proof of ownership: match the exact generator format AND a STARTED
        # event for that mapping/attempt before dropping its definition.
        candidates = process.external_table_candidates(names, settings["mappings"])
        owned = set()
        if candidates:
            events = (spark.table(contract.fq_control(settings, "table_log"))
                      .where((F.col("status") == "STARTED") &
                             F.col("attempt_id").isin([a for _, a in candidates]))
                      .select("mapping_id", "attempt_id").distinct().collect())
            owned = {(r["mapping_id"], r["attempt_id"]) for r in events}
        for key in sorted(owned & candidates.keys()):
            conn.execute(contract.drop_external_sql(settings, candidates[key]), 300)
    print(f"dropped {len(owned & candidates.keys())} owned orphan definitions; "
          f"skipped {len(names) - len(owned & candidates.keys())} unverified definitions")

if do_vacuum and vacuum_failures:
    raise RuntimeError("VACUUM failed for mappings: " + "; ".join(vacuum_failures))
