# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Plan
# MAGIC 1. Checks the reused Synapse staging objects (Parquet format, right container).
# MAGIC 2. Finds tonight's upstream: the row-count manifest CSV in storage, or the
# MAGIC    COMPLETE row in `bridge_upstream.upstream_run` (`upstream.completion_source`).
# MAGIC    Automatic runs require today's Sydney business date and wait up to
# MAGIC    `upstream_wait_minutes`, bounded by the morning deadline.
# MAGIC 3. Reads each table's expected row count from the manifest, so the export does not
# MAGIC    have to scan the table with COUNT_BIG.
# MAGIC 4. Checks every source schema with ONE catalog query.
# MAGIC 5. Packs tables into batches (largest first) and hands the batch ids to the for-each.
# MAGIC
# MAGIC Synapse access here: two tiny catalog lookups, one catalog query for all schemas,
# MAGIC and (only in synapse_table mode) one small upstream lookup. No table data is read.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import json
import time
from datetime import date, timedelta

from bridge import manifest

settings = load_settings()
job_run_id = widget("job_run_id")
business_date_param = widget("business_date")
only = [x.strip() for x in widget("only_mappings").split(",") if x.strip()]
if not contract.KEY.fullmatch(job_run_id):
    raise ValueError("job_run_id parameter is required ({{job.run_id}} in the job definition)")
if business_date_param:
    date.fromisoformat(business_date_param)   # fail fast on a malformed date

enabled_mappings = [m for m in settings["mappings"] if m.get("enabled", True)]
if only:
    unknown = set(only) - {m["mapping_id"] for m in enabled_mappings}
    if unknown:
        raise ValueError(f"only_mappings has unknown or disabled ids: {sorted(unknown)}")
mappings = [m for m in enabled_mappings if not only or m["mapping_id"] in only]
if not mappings:
    raise ValueError("No enabled mappings to plan")

get_props = synapse.credentials_provider(settings, dbutils)
ex = settings["execution"]
tz = ex["cutoff_timezone"]
plan_started = planning.utcnow()
wait_until = plan_started + timedelta(minutes=contract.execution(settings, "upstream_wait_minutes"))
mode = manifest.completion_source(settings)
automatic = not business_date_param
expected_date = planning.business_day(planning.utcnow(), tz) if automatic else date.fromisoformat(business_date_param)
cutoff = planning.nightly_cutoff_utc(
    expected_date, planning.utcnow(), tz, ex["cutoff_local_time"],
    contract.execution(settings, "recovery_cutoff_local_time"), manual=not automatic)
# The plan task has no task retries (they would restart the upstream wait), so
# transient Synapse/storage errors are retried here, for a bounded time and
# never past the cutoff. Deterministic errors still fail at once.
transient_until = min(cutoff, plan_started + timedelta(
    minutes=contract.execution(settings, "plan_transient_retry_minutes")))


def report_transient(exc):
    print(f"Transient platform error; retrying in 60 s: {type(exc).__name__}: {str(exc)[:300]}")


def retry_transient(fn):
    return planning.call_with_transient_retry(fn, synapse.is_transient_platform_error, transient_until,
                                              on_retry=report_transient)


def exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if loader.is_missing_path_error(e):
            return False
        raise


MAX_MARKER_BYTES = 64 * 1024


def read_text(path):
    name = path.rstrip("/").rsplit("/", 1)[-1]
    if name.startswith(("_", ".")):
        # e.g. the _SUCCESS marker: Spark's file sources ignore hidden names.
        text = dbutils.fs.head(path, MAX_MARKER_BYTES + 1)
        if len(text.encode("utf-8")) > MAX_MARKER_BYTES:
            raise ValueError(f"{path} is larger than {MAX_MARKER_BYTES} bytes; not a completion marker")
        return text.lstrip("\ufeff")
    return loader.read_small_text_file(spark, path)


def manifest_ready(d):
    marker = manifest.marker_path(settings, d)
    return (marker is None or exists(marker)) and exists(manifest.path_for(settings, d))


def check_staging():
    with synapse.connect(spark, settings, get_props) as conn:
        # Fail the whole night early (before any table) if the reused Synapse objects are
        # unsuitable: e.g. a delimited-text file format, or a data source pointing elsewhere.
        synapse.check_staging_objects(conn, settings)


retry_transient(check_staging)

# ---- 1. find tonight's upstream --------------------------------------------------------
upstream, mf, manifest_note = None, None, None


def find_upstream():
    """One look for tonight's upstream: (upstream or None, manifest or None, reason)."""
    if mode == "manifest":
        d = expected_date
        if not manifest_ready(d):
            return None, None, f"no manifest found for {d}"
        parsed, note = manifest.read_certified(settings, d, enabled_mappings, read_text,
                                               loader.is_missing_path_error)
        if parsed is None:
            return None, None, f"{note} for {d}"
        return ({"upstream_run_id": parsed.upstream_run_id, "business_date": parsed.business_date,
                 "sequence": parsed.sequence}, parsed, None)
    with synapse.connect(spark, settings, get_props) as conn:
        found = synapse.upstream_run(conn, settings, expected_date.isoformat())
    if found and found["business_date"] == expected_date.isoformat():
        return found, None, None
    return None, None, f"latest COMPLETE run is {found['business_date'] if found else 'none'}"


while True:
    transient = False
    try:
        upstream, mf, latest = find_upstream()
    except Exception as exc:  # noqa: BLE001 - only transient platform errors are retried
        now_utc = planning.utcnow()
        if not synapse.is_transient_platform_error(exc) or now_utc >= max(wait_until, transient_until) \
                or now_utc >= cutoff:
            raise
        report_transient(exc)
        upstream, mf, latest, transient = None, None, f"transient error {type(exc).__name__}", True
    if upstream:
        break
    now_utc = planning.utcnow()
    limit = max(wait_until, transient_until) if transient else wait_until
    if now_utc >= limit or now_utc >= cutoff:
        raise RuntimeError(f"No COMPLETE upstream for expected business date {expected_date} ({latest}). "
                           "Nothing was refreshed; every table keeps its current data.")
    pause = 60 if transient else 300
    print(f"Upstream not ready ({latest}); waiting {pause} s")
    remaining = min((limit - now_utc).total_seconds(), (cutoff - now_utc).total_seconds())
    time.sleep(max(0, min(pause, remaining)))

if planning.utcnow() >= cutoff:
    raise RuntimeError(f"Morning deadline passed while planning {expected_date}; nothing was refreshed")

# ---- 2. row counts from the manifest (synapse_table mode: optional, never blocking) ------
if mode == "synapse_table" and manifest.enabled(settings):
    # The Synapse completion row is the signal here; any manifest problem only
    # means those tables are counted in Synapse (invariant 12).
    mf, manifest_note = manifest.optional_counts(settings, upstream["business_date"], upstream["upstream_run_id"],
                                                 enabled_mappings, manifest_ready, read_text)

# ---- 3. every source schema with one catalog query ---------------------------------------
def read_schema_hashes():
    with synapse.connect(spark, settings, get_props) as conn:
        return synapse.schema_hashes(conn, mappings)


hashes = retry_transient(read_schema_hashes)

# ---- 4. plan ---------------------------------------------------------------------------
release = planning.release_id(settings["source_set_id"], upstream["sequence"])
business_date = date.fromisoformat(upstream["business_date"])
planning.validate_sequence(business_date, upstream["sequence"])
if business_date != expected_date or planning.utcnow() >= cutoff:
    raise RuntimeError(f"Expected business date {expected_date} before {cutoff.isoformat()}, got {business_date}")
counts = mf.counts if mf else {}
by_id = {m["mapping_id"]: m for m in mappings}
batches = planning.make_batches(mappings, ex["max_tables_per_batch"], ex["max_bytes_per_batch"])

rows = []
for b, ids in enumerate(batches):
    for pos, mid in enumerate(ids):
        m = by_id[mid]
        verify = m.get("verify_source_count", True)
        rows.append({"job_run_id": job_run_id, "release_id": release, "batch_id": b, "position": pos,
                     "mapping_id": mid, "mandatory": bool(m.get("mandatory", False)),
                     "business_date": business_date, "upstream_sequence": upstream["sequence"],
                     "cutoff_utc": cutoff.replace(tzinfo=None),
                     "expected_rows": counts.get(mid) if verify else None,
                     "count_source": ("manifest" if mid in counts else "synapse") if verify else "none",
                     "source_schema_hash": hashes.get(mid)})
control.write_plan(spark, settings, rows)

detail = {"tables": len(rows), "batches": len(batches), "cutoff_utc": cutoff.isoformat(),
          "completion_source": mode,
          "manifest": mf.summary([r["mapping_id"] for r in rows]) if mf else None,
          "manifest_note": manifest_note,
          "manifest_warnings": (mf.warnings[:20] if mf else []),
          "schema_objects_missing": sorted(mid for mid, h in hashes.items() if h is None),
          # Runtime 18 ships dated updates on restart: record which build ran.
          "runtime": spark.conf.get(loader.RUNTIME_VERSION_CONF, None)}
control.log_run(spark, settings, release_id=release, job_run_id=job_run_id, event="PLANNED",
                business_date=business_date, upstream_run_id=upstream["upstream_run_id"],
                upstream_sequence=upstream["sequence"], inventory_version=settings["inventory_version"],
                detail=json.dumps(detail)[:4000])

dbutils.jobs.taskValues.set("batch_ids", list(range(len(batches))))
dbutils.jobs.taskValues.set("release_id", release)
print(json.dumps({"release_id": release, **upstream, **detail}, indent=2, default=str))
