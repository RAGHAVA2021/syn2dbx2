# Databricks notebook source
# MAGIC %md
# MAGIC # 90 · Finalize
# MAGIC Runs after every batch has finished (run_if ALL_DONE). Summarises the latest
# MAGIC status of every planned table and fails the job if any mandatory table did not
# MAGIC succeed, so the job's failure notifications fire. Tables that failed keep the
# MAGIC previous night's data.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import json
from datetime import timezone

settings = load_settings()
job_run_id = widget("job_run_id")
repair_count = control.parse_repair_count(widget("repair_count"))
fail_on_any = widget("fail_on_any_failure", "false").lower() == "true"

plan = control.read_plan(spark, settings, job_run_id)
if not plan:
    raise RuntimeError(f"No plan found for job run {job_run_id}; the plan task did not complete")
release = plan[0]["release_id"]
outcomes = control.latest_outcomes(spark, settings, release, job_run_id)
summary = planning.summarise(plan, {k: v["status"] for k, v in outcomes.items()})
summary.update(release_id=release, job_run_id=job_run_id, tables=len(plan))
deadline = plan[0]["cutoff_utc"]
# The finalize task itself may run after the plan's 07:00 or 11:00 cutoff even when all tables were
# published before then. Use each table's Delta commit time, not finalize time
# and not the log event (written after verification, so possibly a little later).
late = planning.late_publications(outcomes, deadline)
deadline_missed = bool(late)
summary["deadline_missed"] = deadline_missed
summary["late_tables"] = late[:50]
summary["ok"] = summary["ok"] and not deadline_missed
fallbacks = control.fallback_counts(spark, settings, job_run_id)
summary["jdbc_fallback_tables"] = fallbacks["total"]
summary["cetas_error_fallback_tables"] = fallbacks["cetas_errors"]
summary["timestamp_reroute_tables"] = fallbacks["timestamp_reroutes"]
fallback_alert = fallbacks["cetas_errors"] > contract.execution(settings, "fallback_alert_threshold")
summary["fallback_alert"] = fallback_alert
summary["ok"] = summary["ok"] and not fallback_alert
# Safety net independent of the mandatory flags (the generator marks every
# mapping optional) and of fail_on_any_failure (default false): a night where
# many tables did not refresh must never look green.
statuses = {k: v["status"] for k, v in outcomes.items()}
ratio = planning.failure_ratio(plan, statuses)
max_ratio = contract.execution(settings, "max_failed_table_ratio")
summary["failed_table_ratio"] = round(ratio, 4)
summary["failure_ratio_alert"] = ratio > max_ratio
summary["ok"] = summary["ok"] and not summary["failure_ratio_alert"]
summary["runtime"] = spark.conf.get(loader.RUNTIME_VERSION_CONF, None)

control.log_run(spark, settings, release_id=release, job_run_id=job_run_id,
                event="FINISHED" if summary["ok"] else "FINISHED_WITH_FAILURES",
                business_date=plan[0]["business_date"], upstream_sequence=plan[0]["upstream_sequence"],
                inventory_version=settings["inventory_version"], detail=json.dumps(summary)[:4000])
print(json.dumps(summary, indent=2, default=str))
dbutils.jobs.taskValues.set("summary", {"counts": summary["counts"], "ok": summary["ok"]})

# One complete notification after every planned table has reached an outcome.
# This is independent of whether the overall job fails: optional failures must
# still reach the morning operator. The receiver deduplicates notification_id.
payload = notification.summary_payload(
    plan, summary, repair_count, contract.execution(settings, "recovery_cutoff_local_time"),
    contract.execution(settings, "cutoff_timezone"))
notification.send_summary(dbutils, settings, payload)
control.log_run(spark, settings, release_id=release, job_run_id=job_run_id,
                event="SUMMARY_NOTIFIED", business_date=plan[0]["business_date"],
                upstream_sequence=plan[0]["upstream_sequence"],
                inventory_version=settings["inventory_version"],
                detail=json.dumps({"notification_id": payload["notification_id"]}))

if deadline_missed:
    raise RuntimeError("Morning deadline missed; inspect per-table publication times and rerun plan")
if fallback_alert:
    raise RuntimeError(f"CETAS-error fallback threshold exceeded: {fallbacks['cetas_errors']} tables; "
                       "inspect CETAS errors")
if summary["failure_ratio_alert"]:
    raise RuntimeError(f"{ratio:.1%} of {len(plan)} planned tables did not refresh (limit {max_ratio:.1%}); "
                       "previous data still served. Failures: " +
                       json.dumps((summary["mandatory_failures"] + summary["other_failures"])[:50]))
if summary["mandatory_failures"] or (fail_on_any and summary["other_failures"]):
    raise RuntimeError("Tables not refreshed (previous data still served): " +
                       json.dumps(summary["mandatory_failures"] + (summary["other_failures"] if fail_on_any else [])))
