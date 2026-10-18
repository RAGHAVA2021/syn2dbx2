# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · Serverless Unity Catalog JDBC fallback
# MAGIC Runs only for mappings whose serverless CETAS phase logged `FALLBACK_REQUIRED`
# MAGIC (or whose previous fallback attempt died after `FALLBACK_STARTED`).
# MAGIC Reads Synapse through a governed Unity Catalog JDBC connection, stages Parquet,
# MAGIC validates it, proves the row count and logs `VALIDATED`. Deterministic failures
# MAGIC end with their real status; transient ones end `FAILED_EXPORT`/`FAILED_COUNT_MISMATCH`
# MAGIC (method JDBC), which the task retry or a Repair run re-attempts.
# MAGIC Every Synapse read is bounded by the publication deadline.
# MAGIC No raw JDBC URL, password, token, or driver is handled by this notebook.
# MAGIC The logic lives in `bridge/fallback.py` (unit-tested).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import json
from datetime import timezone

from bridge import fallback

settings = load_settings()
job_run_id = widget("job_run_id")
batch_id = int(widget("batch_id"))
plan = control.read_plan(spark, settings, job_run_id, batch_id)
if not plan:
    raise RuntimeError(f"No plan rows for job run {job_run_id}, batch {batch_id}")
first = plan[0]
release = first["release_id"]
connection_name = settings["synapse"].get("uc_jdbc_connection")
if not connection_name:
    raise RuntimeError("synapse.uc_jdbc_connection is required by this compute design")

latest = control.latest_status(spark, settings, release, job_run_id)
failed = control.failed_fallbacks(spark, settings, release, job_run_id)
if not fallback.pending(plan, latest, failed):
    print("No FALLBACK_REQUIRED mappings in this batch.")
    dbutils.notebook.exit("{}")

ctx = process.Context(settings=settings, release=release, sequence=int(first["upstream_sequence"]),
                      business_date=str(first["business_date"]), job_run_id=job_run_id,
                      cutoff_utc=first["cutoff_utc"].replace(tzinfo=timezone.utc))
extra_access_codes = contract.execution(settings, "additional_access_error_codes")


class Ops:
    """Real platform operations for bridge.fallback."""

    def export(self, m, stage_path, timeout):
        col = contract.partition_column(m)
        bounds = None
        if col is not None:
            row = loader.uc_jdbc_row(spark, connection_name, contract.bounds_sql(m), timeout)
            if row is not None and row[0] is not None and row[1] is not None:
                bounds = (str(row[0]).strip(), str(row[1]).strip())
        loader.uc_jdbc_export(spark, connection_name, contract.jdbc_select_sql(m), stage_path,
                              timeout=timeout, fetch_size=contract.execution(settings, "jdbc_fetch_size"),
                              partition_column=col["name"] if col else None, bounds=bounds,
                              num_partitions=contract.execution(settings, "jdbc_num_partitions"))

    def count(self, m, timeout):
        return loader.uc_jdbc_count(spark, connection_name, contract.count_sql(m), timeout)

    def read_export(self, path, m):
        # A missing folder raises (never an empty export); _SUCCESS proves a complete write.
        names = [f.name.rstrip("/") for f in dbutils.fs.ls(path)]
        return loader.read_export(spark, path, m, lambda _: names, require_success=True)

    validate_and_convert = staticmethod(loader.validate_and_convert)
    sql_error_info = staticmethod(synapse.sql_error_info)

    def classify_export_error(self, code, state):
        return synapse.classify_export_error(code, state, extra_access_codes)

    def log(self, **fields):
        control.log_table(spark, settings, **fields)


results = fallback.run_batch(ctx, plan, {m["mapping_id"]: m for m in settings["mappings"]}, latest, Ops(),
                             failed_fallbacks=failed)
print(json.dumps(results, indent=2, sort_keys=True))
retry = fallback.retry_needed(results)
if retry:
    raise RuntimeError(f"Serverless UC-JDBC fallback failed transiently (task retry re-runs only these): {retry}")
