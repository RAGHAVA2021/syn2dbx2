# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Extract and validate one batch
# MAGIC Runs once per for-each input. Tables in the batch are processed one after
# MAGIC another: export with CETAS, validate the Parquet, overwrite the target, verify.
# MAGIC Per table, Synapse normally sees only the CETAS itself: the schema was checked
# MAGIC in the plan and the row count comes from the upstream manifest.
# MAGIC The for-each concurrency is therefore the maximum number of concurrent CETAS
# MAGIC statements on the dedicated pool.
# MAGIC
# MAGIC Deterministic failures are logged and do not fail the task. Transient failures
# MAGIC fail the task at the end so the task retry re-runs them; tables that already
# MAGIC reached a terminal status for this release are skipped on the retry.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import json
from contextlib import contextmanager
from datetime import timezone

settings = load_settings()
job_run_id = widget("job_run_id")
batch_id = int(widget("batch_id"))
repair_count = control.parse_repair_count(widget("repair_count"))
plan = control.read_plan(spark, settings, job_run_id, batch_id)
if not plan:
    raise RuntimeError(f"No plan rows for job run {job_run_id}, batch {batch_id}")

first = plan[0]
ctx = process.Context(settings=settings, release=first["release_id"], sequence=int(first["upstream_sequence"]),
                      business_date=str(first["business_date"]), job_run_id=job_run_id,
                      cutoff_utc=first["cutoff_utc"].replace(tzinfo=timezone.utc),
                      # From the plan: schema already checked by one catalog query; row counts
                      # from the upstream manifest. Neither needs a per-table Synapse read.
                      table_info={p["mapping_id"]: {"schema_hash": p["source_schema_hash"],
                                                    "expected_rows": p["expected_rows"]} for p in plan},
                      # Verified earlier publications of this release take precedence over
                      # both a manifest count and a live recount when rechecking a commit.
                      verified_publications=control.verified_publications(
                          spark, settings, first["release_id"], job_run_id,
                          [p["mapping_id"] for p in plan]),
                      # A Repair run keeps the job run id: commit ownership also needs
                      # the repair attempt, and no automatic RESTORE on a live count once
                      # finalize has released the upstream write fence.
                      repair_count=repair_count,
                      fence_released=control.fence_released(spark, settings, job_run_id))
by_id = {m["mapping_id"]: m for m in settings["mappings"]}
missing = [p["mapping_id"] for p in plan if p["mapping_id"] not in by_id]
if missing:
    raise RuntimeError(f"Planned mappings not in the deployed inventory: {missing}")
mappings = [by_id[p["mapping_id"]] for p in plan]
get_props = synapse.credentials_provider(settings, dbutils)
extra_access_codes = contract.execution(settings, "additional_access_error_codes")


def _list_names(path):
    try:
        return [f.name.rstrip("/") for f in dbutils.fs.ls(path)]
    except Exception as e:
        # One type-based classifier for every missing-path check (Runtime 18
        # changed exception types; the acceptance job proves it on the runtime).
        if loader.is_missing_path_error(e):
            raise FileNotFoundError(path) from e
        raise


class Ops:
    """Real platform operations for bridge.process."""

    @contextmanager
    def synapse(self):
        with synapse.connect(spark, settings, get_props) as conn:
            yield conn

    schema_hash = staticmethod(synapse.schema_hash)
    sql_error_info = staticmethod(synapse.sql_error_info)

    def classify_export_error(self, code, state):
        return synapse.classify_export_error(code, state, extra_access_codes)
    count_rows = staticmethod(synapse.count_rows)

    def export(self, conn, m, external_table, location, timeout):
        synapse.export(conn, m, settings, external_table, location, timeout)

    def history(self, m):
        return loader.history(spark, m, contract.execution(settings, "history_limit"))

    def check_target_schema(self, m):
        loader.check_target_schema(spark, m)
        loader.check_target_governance(spark, m, settings)

    def read_export(self, path, m):
        return loader.read_export(spark, path, m, _list_names,
                                  require_success=path.rstrip("/").endswith("-jdbc"))

    validate_and_convert = staticmethod(loader.validate_and_convert)

    def overwrite(self, m, converted, sequence, metadata):
        loader.overwrite(spark, m, converted, sequence, metadata)

    def restore(self, m, version, metadata):
        loader.restore(spark, m, version, metadata)

    def log(self, **fields):
        control.log_table(spark, settings, **fields)

    def delete_stage(self, path):
        dbutils.fs.rm(path, True)

    def reusable_stage(self, ctx, m):
        row = spark.sql(
            f"SELECT method, stage_path FROM {contract.fq_control(settings, 'table_log')} "
            f"WHERE release_id = '{control._safe(ctx.release)}' "
            f"AND job_run_id = '{control._safe(ctx.job_run_id)}' "
            f"AND mapping_id = '{control._safe(m['mapping_id'])}' AND status = 'VALIDATED' "
            "ORDER BY event_utc DESC LIMIT 1").first()
        if row is None or row["method"] not in ("CETAS", "JDBC") or not row["stage_path"]:
            return None
        try:
            dbutils.fs.ls(row["stage_path"])
        except Exception as e:
            if loader.is_missing_path_error(e):
                return None
            raise
        return row["method"], row["stage_path"]


already = control.latest_status(spark, settings, ctx.release, job_run_id)
results = process.process_batch(ctx, mappings, Ops(), already=already, validate_only=True)
print(json.dumps(results, indent=2, sort_keys=True))

retry = process.transient_failures(results)
if retry:
    raise RuntimeError(f"Transient failures (the task retry will re-run only these): {retry}")
