"""Deferred JDBC fallback for one table (serverless, Unity Catalog JDBC connection).

Runs only for mappings the extraction phase marked FALLBACK_REQUIRED. It stages the
source as Parquet, validates it exactly like the CETAS route, proves the row count,
and logs VALIDATED so the publish task can overwrite the target from the stage.

Outcomes:
  * VALIDATED                     staged, validated and counted;
  * a deterministic status        (FAILED_CONTRACT, REJECTED_EMPTY, FAILED_SOURCE_DATA,
                                   FAILED_SCHEMA_DRIFT, FAILED_SOURCE_ACCESS): terminal,
                                   retrying the same read cannot succeed;
  * SKIPPED_CUTOFF                the publication deadline leaves no time to publish;
  * FAILED_EXPORT / FAILED_COUNT_MISMATCH / FAILED_UNKNOWN
                                  a transient failure, logged with method JDBC: this
                                  task's retry (or a Repair run) tries again. If the
                                  retry also fails, this is the table's final status,
                                  as AGENTS.md requires when both routes fail.

The JDBC read is bounded by the publication deadline (queryTimeout), so a slow
fallback cannot hold the publish phase of every other table past the cutoff.

All platform calls go through `ops` so the flow is unit-tested with fakes.
"""
from datetime import datetime, timedelta, timezone

from . import contract, planning, process

# Latest statuses that make a mapping eligible for this task. FALLBACK_STARTED is
# included: a task that dies mid-read (OOM, lost session) leaves it as the latest
# event, and without it the retry would silently abandon the table.
RETRYABLE = ("FALLBACK_REQUIRED", "FALLBACK_STARTED")


def pending(plan, latest, failed_fallbacks=()):
    """Mapping ids of this batch the fallback task must (re)attempt, in plan order.

    failed_fallbacks: mappings whose latest event is a transient failure of an
    earlier fallback attempt of this job run (control.failed_fallbacks).
    """
    failed = set(failed_fallbacks or ())
    return [p["mapping_id"] for p in plan
            if latest.get(p["mapping_id"]) in RETRYABLE or p["mapping_id"] in failed]


class _Transient(RuntimeError):
    def __init__(self, message, status="FAILED_EXPORT", code=None):
        super().__init__(message)
        self.status, self.code = status, code


def _classify(ops, exc, route):
    """Deterministic source failures become ContractError; everything else is transient."""
    code, state = ops.sql_error_info(exc)
    kind = ops.classify_export_error(code, state)
    if kind == "DATA":
        from .synapse import DATA_ERROR_CODES
        return contract.ContractError("FAILED_SOURCE_DATA", f"source data error {code} "
                                      f"({DATA_ERROR_CODES.get(code, 'data')}) during {route}: {exc}")
    if kind == "SCHEMA":
        return contract.ContractError("FAILED_SCHEMA_DRIFT", f"{route} source SQL invalid (error {code}): {exc}")
    if kind == "ACCESS":
        return contract.ContractError("FAILED_SOURCE_ACCESS", f"{route} access denied (error {code}): {exc}")
    return _Transient(f"{route} failed ({kind}, error {code}, SQLSTATE {state}): {type(exc).__name__}: {exc}",
                      code=code)


def run_table(ctx, m, plan_row, ops, now=None, clock=None):
    """Stage, validate and count one table through the UC JDBC connection; return the status."""
    now = now or datetime.now(timezone.utc)
    attempt = process.new_attempt_id(now)
    stage_rel = process.fallback_location(process.stage_location(ctx, m, attempt))
    stage_path = f"{ctx.settings['staging']['root']}/{stage_rel}"
    base = dict(release_id=ctx.release, job_run_id=ctx.job_run_id, mapping_id=m["mapping_id"],
                target=contract.fq_target(m).replace("`", ""), mandatory=bool(m.get("mandatory", False)),
                attempt_id=attempt, stage_path=stage_path, method="JDBC")

    # A read that could never be published only loads the pool. The publish task
    # applies the same deadline, so this skip loses nothing.
    margin = contract.execution(ctx.settings, "publish_margin_minutes")
    deadline = ctx.cutoff_utc - timedelta(minutes=margin)
    if now >= deadline:
        ops.log(**base, status="SKIPPED_CUTOFF",
                detail=f"publication margin ({margin} min before cutoff) reached before JDBC fallback; "
                       "previous data kept")
        return "SKIPPED_CUTOFF"

    ops.log(**base, status="FALLBACK_STARTED", detail="serverless Unity Catalog JDBC fallback started")
    counts = {}

    def timeout():
        # Each Synapse read is bounded by the publication deadline as well as the
        # export timeout: a read that ends after the deadline can never publish.
        left = int((deadline - (clock() if clock else datetime.now(timezone.utc))).total_seconds())
        seconds = min(ctx.settings["execution"]["export_timeout_seconds"], left)
        if seconds <= 0:
            raise _Transient("publication deadline reached during the JDBC fallback", status="SKIPPED_CUTOFF")
        return seconds

    try:
        try:
            ops.export(m, stage_path, timeout())
        except _Transient:
            raise
        except Exception as exc:
            raise _classify(ops, exc, "UC-JDBC fallback read") from exc
        raw = ops.read_export(stage_path, m)
        converted, rows = ops.validate_and_convert(raw, m, ctx.business_date, ctx.release)
        counts["parquet_rows"] = int(rows)

        verify = m.get("verify_source_count", True)
        expected = plan_row.get("expected_rows")
        count_source = "none"
        if verify:
            if expected is not None and int(expected) == int(rows):
                counts["source_rows"], count_source = int(expected), "manifest"
            else:
                # No manifest count, or the manifest disagrees: ask Synapse, through the
                # same governed connection, for this table only.
                try:
                    actual = int(ops.count(m, timeout()))
                except _Transient:
                    raise
                except Exception as exc:
                    raise _classify(ops, exc, "UC-JDBC source count") from exc
                if actual != int(rows):
                    raise _Transient(f"fallback row mismatch: manifest={expected}, staged={rows}, Synapse={actual}",
                                     status="FAILED_COUNT_MISMATCH")
                counts["source_rows"] = actual
                count_source = "uc_jdbc" if expected is None else "uc_jdbc_after_manifest_mismatch"
        if int(rows) == 0 and not m.get("allow_empty", False):
            raise contract.ContractError("REJECTED_EMPTY", "fallback export has 0 rows and allow_empty is false")
    except contract.ContractError as exc:
        # Deterministic: log the real status so finalize reports the actual cause.
        code = ops.sql_error_info(exc.__cause__)[0] if exc.__cause__ is not None else None
        ops.log(**base, **counts, status=exc.status, error_code=code, detail=str(exc))
        return exc.status
    except _Transient as exc:
        # method=JDBC with a transient status marks a failed fallback attempt that the
        # retry re-attempts (control.failed_fallbacks). No error_code: a code on
        # FALLBACK_* rows means "CETAS failed", which only the extraction phase knows.
        ops.log(**base, **counts, status=exc.status,
                detail=f"serverless UC-JDBC fallback attempt failed: {exc}")
        return exc.status
    except Exception as exc:  # noqa: BLE001 - unexpected: the task retry re-attempts it
        ops.log(**base, **counts, status="FAILED_UNKNOWN",
                detail=f"serverless UC-JDBC fallback attempt failed: {type(exc).__name__}: {exc}")
        return "FAILED_UNKNOWN"

    ops.log(**base, **counts, status="VALIDATED", count_source=count_source,
            detail="serverless UC-JDBC fallback staged and validated")
    return "VALIDATED"


def run_batch(ctx, plan, mappings_by_id, latest, ops, clock=None, failed_fallbacks=()):
    """Run every pending mapping of the batch; returns {mapping_id: status}.

    One table's platform failure (e.g. the control log is briefly unwritable) must
    not hold the rest of the batch hostage: it is reported as FAILED_UNKNOWN so
    the task fails and its retry re-attempts only that table.
    """
    todo = pending(plan, latest, failed_fallbacks)
    missing = [mid for mid in todo if mid not in mappings_by_id]
    if missing:
        raise RuntimeError(f"Planned mappings not in the deployed inventory: {missing}")
    results = {}
    plan_by_id = {p["mapping_id"]: p for p in plan}
    for mid in todo:
        try:
            results[mid] = run_table(ctx, mappings_by_id[mid], plan_by_id[mid], ops,
                                     now=clock() if clock else None, clock=clock)
        except Exception as exc:  # noqa: BLE001 - logging/storage outside run_table's handlers
            results[mid] = "FAILED_UNKNOWN"
            print(f"fallback for {mid} failed outside the table handlers: {type(exc).__name__}: {exc}")
    return results


def retry_needed(results):
    return sorted(k for k, v in results.items() if v in planning.TRANSIENT_FAILURES)
