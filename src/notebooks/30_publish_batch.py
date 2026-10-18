# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Publish validated stages on serverless compute
# MAGIC Reuses the immutable VALIDATED Parquet stage produced by the extraction task.
# MAGIC This notebook performs no new source extraction. It validates the staged files again,
# MAGIC atomically overwrites the existing Unity Catalog Delta target, and verifies Delta history.

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
release = first["release_id"]
# Latest VALIDATED event per mapping in this job run (its stage and proven count),
# and the latest event of any kind. A mapping is publishable when a VALIDATED stage
# exists and it has not yet reached a terminal outcome. Selecting on "latest event
# is VALIDATED" would drop a table on the task retry, because this task's own
# STARTED/FAILED_* events are newer than the VALIDATED row.
_log = contract.fq_control(settings, 'table_log')
_where = f"release_id = '{control._safe(release)}' AND job_run_id = '{control._safe(job_run_id)}'"
validated_rows = spark.sql(
    f"SELECT mapping_id, source_rows, count_source, method, stage_path FROM ("
    f"SELECT mapping_id, source_rows, count_source, method, stage_path, "
    f"row_number() OVER (PARTITION BY mapping_id ORDER BY event_utc DESC) rn "
    f"FROM {_log} WHERE {_where} AND status = 'VALIDATED') WHERE rn = 1").collect()
latest = control.latest_status(spark, settings, release, job_run_id)
planned_ids = {p["mapping_id"] for p in plan}
validated = {r["mapping_id"]: r.asDict() for r in validated_rows
             if r["mapping_id"] in planned_ids and latest.get(r["mapping_id"]) not in planning.TERMINAL}
if not validated:
    print("No publishable VALIDATED stages in this batch; nothing to publish.")
    dbutils.notebook.exit("{}")

ctx = process.Context(
    settings=settings, release=release, sequence=int(first["upstream_sequence"]),
    business_date=str(first["business_date"]), job_run_id=job_run_id,
    cutoff_utc=first["cutoff_utc"].replace(tzinfo=timezone.utc),
    table_info={p["mapping_id"]: {
        "schema_hash": p["source_schema_hash"],
        # Prefer the count already proven by the dedicated extraction/validation phase.
        "expected_rows": (validated.get(p["mapping_id"], {}).get("source_rows")
                          if p["mapping_id"] in validated else p["expected_rows"]),
        # Keep the label of the count the extraction/fallback phase proved.
        "count_source": validated.get(p["mapping_id"], {}).get("count_source"),
        # VALIDATED is logged only after source rows == staged Parquet rows.
        "count_proven": p["mapping_id"] in validated,
    } for p in plan},
    verified_publications=control.verified_publications(
        spark, settings, release, job_run_id, [p["mapping_id"] for p in plan]),
    repair_count=repair_count,
    fence_released=control.fence_released(spark, settings, job_run_id))

by_id = {m["mapping_id"]: m for m in settings["mappings"]}
mappings = [by_id[p["mapping_id"]] for p in plan if p["mapping_id"] in validated]

def _list_names(path):
    try:
        return [f.name.rstrip("/") for f in dbutils.fs.ls(path)]
    except Exception as e:
        if loader.is_missing_path_error(e):
            raise FileNotFoundError(path) from e
        raise

class Ops:
    @contextmanager
    def synapse(self):
        # A reusable VALIDATED stage means _export() returns before touching this handle.
        # Any unexpected request for a live source read fails closed on serverless.
        yield None

    @staticmethod
    def schema_hash(*_):
        raise RuntimeError("serverless publish must not re-query Synapse schema")

    @staticmethod
    def count_rows(*_):
        raise RuntimeError("serverless publish must not re-query Synapse row counts")

    @staticmethod
    def sql_error_info(exc):
        return (None, None)

    @staticmethod
    def classify_export_error(code, state):
        return "UNKNOWN"

    def export(self, *_):
        raise RuntimeError("serverless publish must reuse a VALIDATED stage, never run CETAS")


    def history(self, m):
        return loader.history(spark, m, contract.execution(settings, "history_limit"))

    def check_target_schema(self, m):
        loader.check_target_schema(spark, m)
        loader.check_target_governance(spark, m, settings)

    def read_export(self, path, m):
        return loader.read_export(spark, path, m, _list_names, require_success=path.rstrip("/").endswith("-jdbc"))

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
                raise process.TransientError(
                    "FAILED_EXPORT", f"VALIDATED stage {row['stage_path']} no longer exists (lifecycle rule or "
                    "manual cleanup); publish cannot re-extract. Rerun the extraction for this mapping.") from e
            raise
        return row["method"], row["stage_path"]

# already=VALIDATED makes process_batch reuse the staged export (reuse_staged=True).
# With an empty dict it would try to run CETAS, which this task deliberately forbids.
results = process.process_batch(ctx, mappings, Ops(), already={mid: "VALIDATED" for mid in validated},
                                validate_only=False)
print(json.dumps(results, indent=2, sort_keys=True))
retry = process.transient_failures(results)
if retry:
    raise RuntimeError(f"Transient publish failures (task retry will re-run only these): {retry}")
