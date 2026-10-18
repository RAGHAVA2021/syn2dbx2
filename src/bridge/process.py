"""The per-table flow: precheck -> source checks -> export -> validate -> overwrite -> verify.

Export is CETAS. When CETAS itself fails (not the data, not pool capacity), or
its Parquet holds a pre-1900 timestamp, the table is logged FALLBACK_REQUIRED and
handed to the serverless UC-JDBC fallback task (bridge/fallback.py), which stages
and validates it; the publish phase then reuses that VALIDATED stage. Everything
after the export is identical for both routes.

All platform calls go through an `ops` object so the flow is unit-tested with
fakes; the notebook wires in the real Spark / Synapse implementations.

Guarantee: the target is written at most once per table per night, only after
every check has passed, and only if no newer night is already committed.
"""
import time
import uuid
import re
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

from . import contract, manifest, planning
from .synapse import DATA_ERROR_CODES


def _reserve_fallback(ctx, ops, code):
    used = getattr(ops, "fallback_count", 0)
    if used >= contract.execution(ctx.settings, "max_jdbc_fallbacks_per_batch"):
        raise TransientError("FAILED_EXPORT", "JDBC fallback budget reached for this batch; "
                             f"CETAS error {code} needs investigation", code)
    ops.fallback_count = used + 1


@dataclass
class Context:
    settings: dict
    release: str
    sequence: int
    business_date: str
    job_run_id: str
    cutoff_utc: datetime
    # Per mapping, from the plan: schema_hash (from the one batched catalog query; None =
    # object not found) and expected_rows/count_source (from the upstream manifest).
    table_info: dict = field(default_factory=dict)
    # Prior successful events from other runs, keyed by mapping id. A history
    # check must still prove that exact Delta version and its row metric.
    verified_publications: dict = field(default_factory=dict)
    # Repair attempt of this job run ({{job.repair_count}}). A Repair run keeps
    # the job run id, so ownership of a commit is (job_run_id, repair_count).
    repair_count: int = None
    # True once finalize has logged FINISHED/FINISHED_WITH_FAILURES for this job
    # run: the upstream may then legally rewrite the sources (the write fence is
    # released), so a live count can no longer justify an automatic RESTORE.
    fence_released: bool = False


class DeferredFallback(RuntimeError):
    """CETAS cannot serve this table; a later serverless UC-JDBC task must stage it."""
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


class TransientError(RuntimeError):
    """Worth retrying with a fresh export (the task retry does this)."""

    def __init__(self, status, message, code=None):
        super().__init__(message)
        self.status = status
        self.code = code


def new_attempt_id(now=None):
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def stage_location(ctx, m, attempt_id):
    """Relative path under the external data source (the staging container)."""
    st = ctx.settings["staging"]
    return f"{st['prefix']}/{ctx.business_date}/{ctx.release}/{m['mapping_id']}/{attempt_id}/"


def fallback_location(location):
    return location.rstrip("/") + "-jdbc/"


def external_table_name(m, attempt_id):
    return contract.ident(f"x_{m['mapping_id'].replace('-', '_')[:80]}_{attempt_id.replace('-', '_')}"[:128])


def external_table_candidates(names, mappings):
    """Recognize only names this inventory could have generated; log proof is separate."""
    candidates = {}
    for name in names:
        for m in mappings:
            prefix = "x_" + m["mapping_id"].replace("-", "_")[:80] + "_"
            if not name.startswith(prefix):
                continue
            match = re.fullmatch(r"(\d{8}T\d{6}Z)_([0-9a-f]{8})", name[len(prefix):])
            if match:
                attempt = match[1] + "-" + match[2]
                if external_table_name(m, attempt) == name:
                    candidates[(m["mapping_id"], attempt)] = name
    return candidates


# error_code logged for a CETAS failure that reached the fallback without a SQL
# Server error number (mssql-python keeps only the SQLSTATE). NULL is reserved for
# a historical-timestamp reroute, so control.fallback_counts can tell them apart.
CETAS_ERROR_WITHOUT_NUMBER = 0


def _fallback_error_code(code):
    return CETAS_ERROR_WITHOUT_NUMBER if code is None else code


def _data_error(code, route, exc):
    what = DATA_ERROR_CODES.get(code, "SQLSTATE class 22 data exception")
    return contract.ContractError(
        "FAILED_SOURCE_DATA", f"source data error {code} ({what}) during {route}: {exc}")


def _export(ctx, m, ops, conn, attempt, location, timeout, reuse_staged=True):
    """Run CETAS (or reuse a VALIDATED stage). Returns (method, stage_path).

    A CETAS-specific failure raises DeferredFallback: the serverless UC-JDBC task
    stages that table later.
    """
    root = ctx.settings["staging"]["root"]
    if reuse_staged and hasattr(ops, "reusable_stage"):
        reuse = ops.reusable_stage(ctx, m)
        if reuse:
            prefix = f"{root}/{ctx.settings['staging']['prefix']}/{ctx.business_date}/{ctx.release}/{m['mapping_id']}/"
            if not reuse[1].startswith(prefix):
                raise contract.ContractError("FAILED_CONTRACT", "reusable stage is outside this release's mapping")
            return reuse
    try:
        ops.export(conn, m, external_table_name(m, attempt), location, timeout)
        return "CETAS", f"{root}/{location}"
    except Exception as cetas_error:
        code, state = ops.sql_error_info(cetas_error)
        kind = ops.classify_export_error(code, state)
        if kind == "DATA":
            raise _data_error(code, "CETAS", cetas_error) from cetas_error
        if kind == "SCHEMA":
            raise contract.ContractError("FAILED_SCHEMA_DRIFT", f"source SQL became invalid after planning "
                                         f"(error {code}): {cetas_error}") from cetas_error
        if kind == "ACCESS":
            raise contract.ContractError("FAILED_SOURCE_ACCESS", f"Synapse denied export access "
                                         f"(error {code}): {cetas_error}") from cetas_error
        if kind != "CETAS" or not contract.execution(ctx.settings, "jdbc_fallback_enabled"):
            raise TransientError("FAILED_EXPORT", f"CETAS failed ({kind}, error {code}): {cetas_error}",
                                 code) from cetas_error
        _reserve_fallback(ctx, ops, code)
        what = f"error {code}" if code is not None else f"SQLSTATE {state} (no error number)"
        # Logged once, by process_table's DeferredFallback handler (with the code).
        raise DeferredFallback(f"CETAS failed with {what}; deferred JDBC fallback required: "
                               f"{cetas_error}", _fallback_error_code(code)) from cetas_error


def process_table(ctx, m, ops, now=None, monotonic=time.monotonic, current_time=None, reuse_staged=True, validate_only=False):
    """Process one table; returns the final status. Logs every outcome via ops.log."""
    if current_time is None:
        # Bind a caller-supplied `now` by value. Closing over the local `now`
        # (reassigned below) would freeze the live clock at table start, so the
        # pre-publication cutoff check and remaining() could never advance.
        current_time = (lambda fixed=now: fixed) if now is not None else (lambda: datetime.now(timezone.utc))
    now = now or current_time()
    started = monotonic()
    base = {"release_id": ctx.release, "job_run_id": ctx.job_run_id, "mapping_id": m["mapping_id"],
            "target": contract.fq_target(m).replace("`", ""), "mandatory": bool(m.get("mandatory", False))}
    attempt = new_attempt_id(now)
    location = stage_location(ctx, m, attempt)
    base.update(attempt_id=attempt, stage_path=f"{ctx.settings['staging']['root']}/{location}")
    def log_with_retry(**fields):
        for retry in range(3):
            try:
                ops.log(**fields)
                return
            except Exception:
                if retry == 2:
                    raise
                time.sleep(0.2 * (2 ** retry))

    log_with_retry(**base, status="STARTED")
    counts, extra = {}, {}

    def final(status, detail, **more):
        fields = {**base, **counts, **extra, **more}   # later keys win (e.g. the fallback stage_path)
        fields.update(status=status, detail=detail, duration_seconds=round(monotonic() - started, 1))
        log_with_retry(**fields)
        return status

    # New exports and publications both stop publish_margin_minutes before the
    # cutoff: an export started inside the margin could never be published, so
    # it would only load the pool and be thrown away.
    margin = contract.execution(ctx.settings, "publish_margin_minutes")
    publish_deadline = ctx.cutoff_utc - timedelta(minutes=margin)

    def remaining():
        return max(0, math.ceil((publish_deadline - current_time()).total_seconds()))

    def bounded_timeout():
        seconds = min(ctx.settings["execution"]["export_timeout_seconds"], remaining())
        if seconds <= 0:
            raise TransientError("SKIPPED_CUTOFF", "cutoff reached before source count")
        return seconds

    def verification_timeout():
        # A commit may already be live after the deadline. Read-only verification
        # must finish even though the deadline bars any new export or publication.
        return ctx.settings["execution"]["export_timeout_seconds"]

    try:
        ops.check_target_schema(m)   # first: a missing table is deterministic, not a history error
        hist = ops.history(m)
        decided = planning.precheck(hist, ctx.sequence, ctx.business_date)
        if decided:
            if decided == "FAILED_UPSTREAM_SEQUENCE":
                return final(decided, "Delta history sequence/date conflicts with requested upstream release")
            if decided == "ALREADY_CURRENT":
                return _verify_existing_commit(ctx, m, ops, hist, final, counts, extra, verification_timeout)
            return final(decided, "target already has a newer night; nothing exported" if decided == "SUPERSEDED"
                         else "target publication was restored or changed; investigate before republishing")

        # A retry may start after the deadline even though an earlier attempt
        # committed successfully. Verify that existing commit before recording
        # a cutoff skip; never let a later skip hide a live publication.
        if now >= publish_deadline:
            return final("SKIPPED_CUTOFF", f"publication margin ({margin} min before cutoff) reached before "
                                           "export; previous data kept")

        timeout = min(ctx.settings["execution"]["export_timeout_seconds"], remaining())
        if timeout <= 0:
            return final("SKIPPED_CUTOFF", "cutoff reached before source checks")
        info = ctx.table_info.get(m["mapping_id"], {})

        # Source schema: normally already checked by the plan's single catalog query.
        planned_hash = "schema_hash" in info
        if planned_hash and info["schema_hash"] != m["source_schema_hash"]:
            actual = info["schema_hash"]
            raise contract.ContractError(
                "FAILED_SCHEMA_DRIFT",
                f"source object {m['source_schema']}.{m['source_object']} not found" if actual is None
                else f"source schema hash {actual} != inventory {m['source_schema_hash']}")

        # Source row count: from the upstream manifest when it has one (no Synapse scan).
        verify = m.get("verify_source_count", True)
        count_source = None
        if verify and info.get("expected_rows") is not None:
            counts["source_rows"] = int(info["expected_rows"])
            count_source = "manifest"
            # A later phase (serverless publish) reuses a count an earlier phase proved;
            # keep that label instead of calling it a manifest count. Any label other
            # than "manifest" is already verified, so a mismatch is never re-counted.
            if info.get("count_source") and info["count_source"] != "manifest":
                count_source = info["count_source"]
        if count_source == "manifest" and counts["source_rows"] == 0 and not m.get("allow_empty", False):
            # A manifest zero can be wrong. Confirm it before marking the table
            # terminal; otherwise a nonempty source never even reaches CETAS.
            try:
                with ops.synapse() as conn:
                    actual_rows = ops.count_rows(conn, m, bounded_timeout())
            except TransientError:
                raise
            except Exception as e:
                raise TransientError("FAILED_EXPORT", f"could not confirm manifest zero in Synapse: {e}") from e
            counts["source_rows"] = actual_rows
            count_source = "synapse_after_manifest_zero"
            if actual_rows == 0:
                raise contract.ContractError("REJECTED_EMPTY", "source has 0 rows (confirmed in Synapse)")

        try:
            with ops.synapse() as conn:
                if not planned_hash:
                    actual = ops.schema_hash(conn, m)
                    if actual != m["source_schema_hash"]:
                        raise contract.ContractError(
                            "FAILED_SCHEMA_DRIFT", f"source schema hash {actual} != inventory {m['source_schema_hash']}")
                if verify and count_source is None:
                    counts["source_rows"] = ops.count_rows(conn, m, timeout)
                    count_source = "synapse"
                    if counts["source_rows"] == 0 and not m.get("allow_empty", False):
                        raise contract.ContractError("REJECTED_EMPTY", "source has 0 rows and allow_empty is false")
                export_timeout = min(timeout, remaining())
                if export_timeout <= 0:
                    return final("SKIPPED_CUTOFF", "cutoff reached before export")
                method, stage_path = _export(ctx, m, ops, conn, attempt, location, export_timeout,
                                             reuse_staged)
        except (contract.ContractError, TransientError, DeferredFallback):
            raise
        except Exception as e:   # connection/schema/count problems before or around the export
            code, _ = ops.sql_error_info(e)
            raise TransientError("FAILED_EXPORT", f"source access failed (error {code}): {e}", code) from e
        extra.update(method=method, stage_path=stage_path, count_source=count_source)

        raw = ops.read_export(stage_path, m)
        try:
            converted, rows = ops.validate_and_convert(raw, m, ctx.business_date, ctx.release,
                                                       reject_ancient_timestamps=(method == "CETAS"))
        except contract.ContractError as e:
            if not isinstance(e, contract.AncientCetasTimestamp) or method != "CETAS" or not \
                    contract.execution(ctx.settings, "jdbc_fallback_enabled"):
                raise
            # Historical sentinels are a contract route, not a CETAS outage: hand the
            # table to the UC-JDBC task without using the per-batch error-fallback budget.
            log_with_retry(**base, status="FALLBACK_REQUIRED", method="JDBC",
                           detail="CETAS historical timestamp requires deferred JDBC fallback")
            return "FALLBACK_REQUIRED"
        counts["parquet_rows"] = rows
        note = ""
        if "source_rows" in counts and rows != counts["source_rows"]:
            if count_source != "manifest" or info.get("count_proven"):
                raise TransientError("FAILED_COUNT_MISMATCH",
                                     f"exported {rows} rows via {method}, source had {counts['source_rows']}")
            # The manifest disagrees: only now ask Synapse (one scan, only for this table).
            try:
                with ops.synapse() as conn:
                    actual_rows = ops.count_rows(conn, m, bounded_timeout())
            except TransientError:
                raise
            except Exception as e:
                raise TransientError("FAILED_COUNT_MISMATCH",
                                     f"manifest says {counts['source_rows']}, exported {rows}; "
                                     f"Synapse verification count failed: {e}") from e
            if actual_rows != rows:
                raise TransientError("FAILED_COUNT_MISMATCH",
                                     f"exported {rows} rows via {method}; manifest says {counts['source_rows']}, "
                                     f"Synapse says {actual_rows}")
            note = (f"manifest count {counts['source_rows']} was wrong; Synapse confirmed {actual_rows} "
                    f"(= exported). ")
            counts["source_rows"] = actual_rows
            extra["count_source"] = "synapse_after_manifest_mismatch"
        if rows == 0 and not m.get("allow_empty", False):
            raise contract.ContractError("REJECTED_EMPTY", "export has 0 rows and allow_empty is false")

        # Immutable stage is now fully verified. A task retry after a failed
        # Delta write can re-read and revalidate it without a second CETAS.
        log_with_retry(**{**base, **counts, **extra, "status": "VALIDATED"})
        if validate_only:
            return "VALIDATED"

        # A long export can cross the morning deadline after the initial check.
        # Keep yesterday's committed data instead of publishing too late.
        if current_time() >= publish_deadline:
            return final("SKIPPED_CUTOFF", "publication margin reached before overwrite; previous data kept")

        metadata = planning.user_metadata(ctx.release, ctx.sequence, ctx.business_date,
                                          m["mapping_id"], ctx.job_run_id, attempt, ctx.repair_count)
        write_error = None
        try:
            ops.overwrite(m, converted, ctx.sequence, metadata)
        except Exception as e:
            write_error = e   # the commit may or may not have happened; history decides

        hist = ops.history(m)
        status, version, detail = planning.verify_after_write(hist, ctx.release, ctx.sequence, rows,
                                                             ctx.business_date)
        if status == "FAILED_VERIFY_NEEDS_ROLLBACK":
            status, version, detail = restore_bad_commit(ctx, m, ops, hist, version, detail)
        elif status == "FAILED_WRITE":
            raise TransientError("FAILED_WRITE", f"{detail}: {write_error}" if write_error else detail)
        result = final(status, note + detail, delta_version=version,
                       written_rows=rows if status == "PUBLISHED" else None,
                       commit_utc=planning.commit_utc(hist, version) if status == "PUBLISHED" else None)
        if result == "PUBLISHED" and hasattr(ops, "delete_stage"):
            stale = (stage_path, base["stage_path"])
            if stage_path != base["stage_path"]:
                # A stage reused from an earlier phase: the CETAS attempt folder that
                # preceded a JDBC fallback has another attempt id. Every attempt of
                # this release/mapping is now obsolete.
                st = ctx.settings["staging"]
                stale = (f"{st['root']}/{st['prefix']}/{ctx.business_date}/{ctx.release}/{m['mapping_id']}/",)
            for stale_path in dict.fromkeys(stale):
                try:
                    ops.delete_stage(stale_path)
                except Exception as cleanup_error:
                    # Lifecycle handles leftover files after a verified commit.
                    print(f"staging cleanup failed for {m['mapping_id']}: {cleanup_error}")
        return result
    except DeferredFallback as e:
        return final("FALLBACK_REQUIRED", str(e), error_code=e.code, method="JDBC")
    except contract.ContractError as e:
        return final(e.status, str(e))
    except TransientError as e:
        return final(e.status, str(e), error_code=e.code)
    except Exception as e:  # anything unexpected is retried by the task retry
        # Condition first (Runtime 18 changed exception types); message text as a fallback.
        if "READ_ANCIENT_DATETIME" in f"{manifest.error_condition(e)} {e}":
            return final("FAILED_CONTRACT", f"Parquet ancient date/timestamp encoding: {e}")
        return final("FAILED_UNKNOWN", f"{type(e).__name__}: {e}")


def _verify_existing_commit(ctx, m, ops, hist, final, counts, extra, verification_timeout):
    """Verify a commit for this release that is already in the target's history.

    A worker can die after the Delta commit but before it verifies and logs it,
    so history alone must not turn that commit into a success. The evidence for
    the expected row count is taken in this order:

    1. a logged, verified publication of exactly this Delta version and row metric
       (it was proven when it was written; the source may have moved on since);
    2. the upstream manifest count for this release;
    3. a Synapse count now.

    A live count is only trustworthy for a commit made by THIS job run, while
    the upstream is still fenced. A commit left by another job run may be
    hours old, and the source may legitimately have changed since. Such a
    commit is never restored automatically: a disagreement is reported as
    FAILED_VERIFY for a human to inspect.
    """
    mapping_id = m["mapping_id"]
    info = ctx.table_info.get(mapping_id, {})
    verify = m.get("verify_source_count", True)
    commit, commit_md = planning.release_commit(hist, ctx.release)
    metric = ((commit or {}).get("operationMetrics") or {}).get("numOutputRows")
    metric = int(metric) if metric not in (None, "") else None
    # Only this execution's own commit, made while the upstream is still fenced,
    # may be judged by a live count and restored. A Repair run reuses the job
    # run id and runs after finalize released the fence; a commit without a
    # repair_count (older versions) never qualifies once one is expected.
    own_run = (commit_md is not None and str(commit_md.get("job_run_id")) == str(ctx.job_run_id)
               and commit_md.get("repair_count") == ctx.repair_count and not ctx.fence_released)

    expected, source = None, None
    prior = ctx.verified_publications.get(mapping_id)
    if verify and prior and commit is not None and int(commit["version"]) == int(prior["version"]) \
            and metric is not None and metric == int(prior["written_rows"]):
        expected, source = int(prior["written_rows"]), "prior_verified_publication"
    if expected is None and verify and info.get("expected_rows") is not None:
        expected, source = int(info["expected_rows"]), "manifest"
        if info.get("count_proven"):
            source = info.get("count_source") or "validated_stage"
    if expected is None and verify:
        with ops.synapse() as conn:
            expected, source = ops.count_rows(conn, m, verification_timeout()), "synapse"
    if expected is None:
        # Count verification was explicitly disabled for this mapping.
        # Still require an actual output-row metric for the commit.
        if metric is None:
            return final("FAILED_VERIFY", "no output-row metric for existing commit")
        expected, source = metric, "none"
    extra["count_source"] = source
    if source in ("manifest", "synapse") or info.get("count_proven"):
        counts["source_rows"] = expected

    status, version, detail = planning.verify_after_write(hist, ctx.release, ctx.sequence, expected,
                                                          ctx.business_date)
    # A count proven by an earlier phase (VALIDATED only after source == Parquet rows)
    # is not a possibly-wrong manifest value: never recount it. The serverless publish
    # task has no Synapse access, so a recount there could only fail forever.
    if status != "PUBLISHED" and source == "manifest" and not info.get("count_proven"):
        # A wrong manifest can disagree with an otherwise valid commit.
        with ops.synapse() as conn:
            actual = ops.count_rows(conn, m, verification_timeout())
        status, version, detail = planning.verify_after_write(hist, ctx.release, ctx.sequence, actual,
                                                              ctx.business_date)
        counts["source_rows"] = actual
        if status == "PUBLISHED":
            # The Synapse count, not the rejected manifest value, is the proof
            # for this recovered commit and for a later date-based rollback.
            expected = actual
            extra["count_source"] = "synapse_after_manifest_mismatch"
        else:
            extra["count_source"] = "synapse"
    if status == "PUBLISHED":
        return final("ALREADY_CURRENT", "verified existing commit; nothing exported", delta_version=version,
                     written_rows=int(expected), commit_utc=planning.commit_utc(hist, version))
    if status == "FAILED_VERIFY_NEEDS_ROLLBACK":
        if own_run:
            status, version, detail = restore_bad_commit(ctx, m, ops, hist, version, detail)
            return final(status, detail, delta_version=version)
        made_by = (commit_md or {}).get("job_run_id")
        made_repair = (commit_md or {}).get("repair_count")
        same_execution = str(made_by) == str(ctx.job_run_id) and made_repair == ctx.repair_count
        why = ("the upstream write fence was released after this run finalized" if same_execution
               else f"the commit was made by job run {made_by} (repair {made_repair}), not this execution "
                    f"(job run {ctx.job_run_id}, repair {ctx.repair_count})")
        return final("FAILED_VERIFY", f"{detail} (count source: {extra['count_source']}); {why}, "
                                      "and the source may have changed since, "
                                      "so it was NOT restored automatically; inspect, then roll back manually "
                                      "or republish with a higher upstream sequence", delta_version=version)
    return final("FAILED_VERIFY", f"existing commit is unverified: {detail}; inspect and republish "
                                  "with a higher upstream sequence")


def restore_bad_commit(ctx, m, ops, hist, version, detail):
    """Use the same guarded restore for a live bad commit and for a lost ack."""
    to = planning.rollback_target_version(hist, version)
    if to is None:
        return "FAILED_VERIFY", version, f"{detail}; newer commits exist, not restored automatically"
    current = ops.history(m)
    if planning.rollback_target_version(current, version) != to:
        return "FAILED_VERIFY", version, f"{detail}; target changed before restore; not restored"
    metadata = planning.restore_metadata(ctx.release, version, to, "automatic")
    ops.restore(m, to, metadata)
    after = ops.history(m)
    # Do not claim that old data is serving unless the RESTORE is actually the
    # latest commit. A second writer can race with the RESTORE; an exclusive
    # writer window remains a deployment requirement.
    if (not after or after[0].get("operation") != "RESTORE" or after[0].get("userMetadata") != metadata
            or int(after[0]["version"]) != int(version) + 1):
        return "FAILED_VERIFY", version, f"{detail}; RESTORE outcome is not the latest target commit"
    return "FAILED_VERIFY_ROLLED_BACK", version, f"{detail}; restored to version {to}"


def process_batch(ctx, mappings, ops, already=None, clock=None, validate_only=False):
    """Process a batch sequentially; returns {mapping_id: status}.

    Tables already terminal for this release (from an earlier attempt of the
    same task) are skipped, so a task retry only redoes transient failures.
    """
    already = already or {}
    results = {}
    for m in mappings:
        prior = already.get(m["mapping_id"])
        # A published result in the append-only log is not proof that its Delta
        # version is still live: an operator or the verifier may have restored it.
        # Recheck successful results against history on every retry.
        if prior in planning.DETERMINISTIC_FAILURES:
            results[m["mapping_id"]] = prior
            continue
        if prior == "FALLBACK_REQUIRED":
            # Already handed to the deferred fallback task. Re-running CETAS on a task
            # retry would repeat a failed export and spend the fallback budget again.
            results[m["mapping_id"]] = prior
            continue
        try:
            if prior in planning.SUCCESS:
                current = planning.precheck(ops.history(m), ctx.sequence, ctx.business_date)
                if current in ("ALREADY_CURRENT", "SUPERSEDED"):
                    results[m["mapping_id"]] = prior if current == "ALREADY_CURRENT" else current
                    continue
            results[m["mapping_id"]] = process_table(ctx, m, ops, now=clock() if clock else None,
                                                      reuse_staged=prior is not None, validate_only=validate_only)
        except Exception as e:
            # Logging/history can fail outside the table's own exception handlers.
            # Do not hold the remaining tables in this batch hostage.
            results[m["mapping_id"]] = "FAILED_UNKNOWN"
            try:
                ops.log(release_id=ctx.release, job_run_id=ctx.job_run_id, mapping_id=m["mapping_id"],
                        target=contract.fq_target(m).replace("`", ""), mandatory=bool(m.get("mandatory", False)),
                        status="FAILED_UNKNOWN", detail=f"table processing failed: {type(e).__name__}: {e}")
            except Exception:
                pass  # the batch task still fails so the next attempt can retry this table
    return results


def transient_failures(results):
    return sorted(k for k, v in results.items() if v in planning.TRANSIENT_FAILURES)
