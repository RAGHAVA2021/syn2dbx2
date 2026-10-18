"""Append-only control log in Unity Catalog.

Every writer only appends (blind appends never conflict in Delta), so the
concurrent for-each iterations can log without retries or locks. Current
state is derived by the table_latest view (latest event per table and release).
"""
from datetime import datetime, timezone

from . import contract

TABLE_LOG = (
    "release_id string, job_run_id string, mapping_id string, target string, mandatory boolean, "
    "attempt_id string, status string, source_rows bigint, parquet_rows bigint, written_rows bigint, "
    "delta_version bigint, stage_path string, detail string, event_utc timestamp, "
    "method string, error_code int, duration_seconds double, count_source string, commit_utc timestamp")
RUN_LOG = (
    "release_id string, job_run_id string, event string, business_date date, upstream_run_id string, "
    "upstream_sequence bigint, inventory_version string, detail string, event_utc timestamp")
PLAN = (
    "job_run_id string, release_id string, batch_id int, position int, mapping_id string, mandatory boolean, "
    "business_date date, upstream_sequence bigint, cutoff_utc timestamp, planned_utc timestamp, "
    "expected_rows bigint, count_source string, source_schema_hash string")


def field_names(ddl):
    return [part.strip().split()[0] for part in ddl.split(",")]


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _append(spark, settings, table, ddl, rows):
    names = field_names(ddl)
    df = spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], ddl)
    df.write.mode("append").saveAsTable(contract.fq_control(settings, table))


def log_table(spark, settings, **f):
    row = {k: f.get(k) for k in field_names(TABLE_LOG)}
    row["event_utc"] = _now()
    if row.get("detail"):
        row["detail"] = str(row["detail"])[:4000]
    _append(spark, settings, "table_log", TABLE_LOG, [row])


def log_run(spark, settings, **f):
    row = {k: f.get(k) for k in field_names(RUN_LOG)}
    row["event_utc"] = _now()
    _append(spark, settings, "run_log", RUN_LOG, [row])


def write_plan(spark, settings, rows):
    target = contract.fq_control(settings, "plan")
    job_run_id = rows[0]["job_run_id"]
    # A re-run of the plan task in the same job run replaces its own rows only.
    spark.sql(f"DELETE FROM {target} WHERE job_run_id = '{_safe(job_run_id)}'")
    stamped = [{**r, "planned_utc": _now()} for r in rows]
    _append(spark, settings, "plan", PLAN, stamped)


def read_plan(spark, settings, job_run_id, batch_id=None):
    sql = f"SELECT * FROM {contract.fq_control(settings, 'plan')} WHERE job_run_id = '{_safe(job_run_id)}'"
    if batch_id is not None:
        sql += f" AND batch_id = {int(batch_id)}"
    return [r.asDict() for r in spark.sql(sql + " ORDER BY batch_id, position").collect()]


def latest_status(spark, settings, release, job_run_id):
    return {k: v["status"] for k, v in latest_outcomes(spark, settings, release, job_run_id).items()}


def verified_publications(spark, settings, release, job_run_id, mapping_ids):
    """Earlier verified publications for the same release; history remains authoritative.

    This is only a count hint on a new job run when the manifest lacks a count.
    The caller must match the exact Delta version and output-row metric.
    """
    if not mapping_ids:
        return {}
    ids = ",".join(f"'{_safe(mid)}'" for mid in mapping_ids)
    rows = spark.sql(
        f"SELECT mapping_id, delta_version, written_rows, source_rows FROM "
        f"{contract.fq_control(settings, 'table_log')} "
        f"WHERE release_id = '{_safe(release)}' AND job_run_id <> '{_safe(job_run_id)}' "
        f"AND mapping_id IN ({ids}) AND status = 'PUBLISHED' "
        "AND delta_version IS NOT NULL AND written_rows IS NOT NULL "
        "ORDER BY event_utc DESC").collect()
    result = {}
    for row in rows:
        mid = row["mapping_id"]
        if mid not in result and (row["source_rows"] is None or
                                  int(row["source_rows"]) == int(row["written_rows"])):
            result[mid] = {"version": int(row["delta_version"]), "written_rows": int(row["written_rows"])}
    return result


FENCE_RELEASE_EVENTS = ("FINISHED", "FINISHED_WITH_FAILURES")


def fence_released(spark, settings, job_run_id):
    """Whether finalize already released the upstream write fence for this job run.

    The upstream may start a same-day correction once it sees one of these
    events, so a later Repair run of the same job run is no longer fenced.
    """
    events = ",".join(f"'{e}'" for e in FENCE_RELEASE_EVENTS)
    row = spark.sql(
        f"SELECT count(*) AS n FROM {contract.fq_control(settings, 'run_log')} "
        f"WHERE job_run_id = '{_safe(job_run_id)}' AND event IN ({events})").first()
    return bool(row and int(row["n"]))


def parse_repair_count(value):
    """{{job.repair_count}} as an int. Databricks passes an unresolved
    reference through as literal text, which must not become 'no repair'."""
    text = str(value).strip()
    if not text.isdigit():
        raise ValueError(f"repair_count must be {{{{job.repair_count}}}} (a non-negative integer), got {value!r}")
    return int(text)


def fallback_counts(spark, settings, job_run_id):
    """Tables that needed the JDBC fallback, split by cause (one count per mapping).

    The extraction phase logs FALLBACK_REQUIRED, with the CETAS error code when CETAS
    failed and without one for a historical-timestamp reroute; the later fallback
    task logs FALLBACK_STARTED without a code. A mapping with a code on any of these
    events is a CETAS-error fallback; the rest are timestamp reroutes.
    """
    row = spark.sql(
        "SELECT count(*) AS total, coalesce(sum(CASE WHEN has_code THEN 1 ELSE 0 END), 0) AS cetas_errors FROM ("
        "SELECT mapping_id, max(error_code IS NOT NULL) AS has_code "
        f"FROM {contract.fq_control(settings, 'table_log')} "
        f"WHERE job_run_id = '{_safe(job_run_id)}' AND status IN ('FALLBACK_REQUIRED', 'FALLBACK_STARTED') "
        "GROUP BY mapping_id)").first()
    total, cetas = int(row["total"] or 0), int(row["cetas_errors"] or 0)
    return {"cetas_errors": cetas, "timestamp_reroutes": total - cetas, "total": total}


def failed_fallbacks(spark, settings, release, job_run_id):
    """Mappings whose latest event is a transient failure of a JDBC fallback attempt.

    The fallback task logs FAILED_EXPORT / FAILED_COUNT_MISMATCH / FAILED_UNKNOWN with
    method JDBC when an attempt fails. Publish can also log transient failures with
    method JDBC, but only for a mapping that already has a VALIDATED stage, so a
    VALIDATED event excludes the mapping here (publish owns its retry).
    """
    from .planning import TRANSIENT_FAILURES
    log = contract.fq_control(settings, "table_log")
    where = f"release_id = '{_safe(release)}' AND job_run_id = '{_safe(job_run_id)}'"
    statuses = ",".join(f"'{s}'" for s in sorted(TRANSIENT_FAILURES))
    rows = spark.sql(
        "SELECT mapping_id FROM (SELECT mapping_id, status, method, "
        "row_number() OVER (PARTITION BY mapping_id ORDER BY event_utc DESC) AS rn "
        f"FROM {log} WHERE {where}) WHERE rn = 1 AND method = 'JDBC' AND status IN ({statuses}) "
        f"AND mapping_id NOT IN (SELECT mapping_id FROM {log} WHERE {where} AND status = 'VALIDATED')").collect()
    return {r["mapping_id"] for r in rows}


def latest_outcomes(spark, settings, release, job_run_id):
    """Latest event for this job run, never a previous run of the same release.

    A fresh job run must reassess repaired deterministic failures. Reusing a
    prior run's terminal status prevents the documented repair-and-rerun flow.
    """
    rows = spark.sql(
        "SELECT mapping_id, status, event_utc, commit_utc FROM (SELECT mapping_id, status, event_utc, commit_utc, "
        "row_number() OVER (PARTITION BY mapping_id ORDER BY event_utc DESC) AS rn "
        f"FROM {contract.fq_control(settings, 'table_log')} "
        f"WHERE release_id = '{_safe(release)}' AND job_run_id = '{_safe(job_run_id)}') "
        "WHERE rn = 1").collect()
    return {r["mapping_id"]: {"status": r["status"], "event_utc": r["event_utc"], "commit_utc": r["commit_utc"]}
            for r in rows}


def _safe(value):
    value = str(value)
    if not contract.KEY.fullmatch(value):
        raise ValueError(f"Unsafe control key {value!r}")
    return value
