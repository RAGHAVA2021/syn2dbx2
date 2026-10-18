# Databricks notebook source
# MAGIC %md
# MAGIC # Rollback one table
# MAGIC Restores a target to the version published for an earlier business date (or an
# MAGIC explicit Delta version). Only versions still inside
# MAGIC `delta.deletedFileRetentionDuration` can be restored.
# MAGIC
# MAGIC **After a rollback**, the table's idempotency guard still remembers the newer
# MAGIC night, so re-running the same night will be skipped. To republish that night,
# MAGIC record a new upstream run with a higher `upstream_sequence`. The next normal
# MAGIC night (higher sequence) publishes as usual.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

settings = load_settings()
mapping_id = widget("mapping_id")
to_business_date = widget("to_business_date")
to_version = widget("to_version")
expected_current_version = widget("expected_current_version")
reason = widget("reason")
dry_run = widget("dry_run", "true").lower() != "false"
if not reason:
    raise ValueError("reason is required")
if bool(to_business_date) == bool(to_version):
    raise ValueError("Give exactly one of to_business_date or to_version")

m = next((x for x in settings["mappings"] if x["mapping_id"] == mapping_id), None)
if m is None:
    raise ValueError(f"Unknown mapping_id {mapping_id!r}")
loader.check_target_governance(spark, m, settings)
hist = loader.history(spark, m, limit=1000)
current = hist[0]["version"]

if to_version:
    version = int(to_version)
    if not any(h["version"] == version for h in hist):
        raise ValueError(f"Version {version} not found in history")
else:
    from datetime import date
    date.fromisoformat(to_business_date)
    log_rows = spark.sql(
        f"SELECT delta_version, written_rows FROM {contract.fq_control(settings, 'table_log')} "
        f"WHERE mapping_id = '{control._safe(mapping_id)}' AND status IN ('PUBLISHED','ALREADY_CURRENT') "
        "AND delta_version IS NOT NULL AND written_rows IS NOT NULL").collect()
    logged = {int(r["delta_version"]): int(r["written_rows"]) for r in log_rows}
    candidates = planning.rollback_candidates(hist, logged, to_business_date)
    print(f"Verified rollback candidates for {to_business_date}: {candidates}")
    if not candidates:
        raise ValueError(f"No verified, unrestored publication for business date {to_business_date}; "
                         "use an explicit version after investigating the history")
    version = candidates[0]

print(f"{contract.fq_target(m)}: current version {current}, restore to {version}")
if version == current:
    print("Already at that version; nothing to do.")
elif dry_run:
    print(f"dry_run=true: nothing changed. Set expected_current_version={current} "
          "and dry_run=false after stopping all other target writers.")
else:
    if not expected_current_version or not expected_current_version.isdigit():
        raise ValueError("expected_current_version is required for a non-dry-run rollback")
    expected = int(expected_current_version)
    if current != expected:
        raise RuntimeError(f"Target version changed since approval: expected {expected}, found {current}")
    # Re-read immediately before RESTORE. This rejects commits that arrived
    # during the notebook's own inspection; an exclusive write window is still
    # required to close the remaining gap between this read and RESTORE.
    live = loader.history(spark, m, limit=1)[0]["version"]
    if live != expected:
        raise RuntimeError(f"Target version changed before RESTORE: expected {expected}, found {live}")
    loader.restore(spark, m, version, planning.restore_metadata("manual", current, version, reason))
    control.log_table(spark, settings, release_id="manual-rollback", job_run_id="manual", mapping_id=mapping_id,
                      target=contract.fq_target(m).replace("`", ""), status="ROLLED_BACK", delta_version=version,
                      detail=f"from version {current}: {reason}")
    print("Restored.")
