"""Pure planning and decision logic (no Spark, no Synapse): fully unit-tested."""
import json
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ------------------------------------------------------------------ statuses
SUCCESS = {"PUBLISHED", "ALREADY_CURRENT", "SUPERSEDED"}
DETERMINISTIC_FAILURES = {"REJECTED_EMPTY", "FAILED_SCHEMA_DRIFT", "FAILED_SOURCE_ACCESS", "FAILED_CONTRACT", "FAILED_SOURCE_DATA",
                          "FAILED_TARGET_SCHEMA", "FAILED_VERIFY_ROLLED_BACK", "FAILED_VERIFY",
                          "FAILED_UPSTREAM_SEQUENCE"}
TRANSIENT_FAILURES = {"FAILED_EXPORT", "FAILED_COUNT_MISMATCH", "FAILED_WRITE", "FAILED_UNKNOWN"}
# Terminal for this release: a rerun or task retry skips these tables.
TERMINAL = SUCCESS | DETERMINISTIC_FAILURES
IN_FLIGHT = {"STARTED", "FALLBACK_STARTED", "FALLBACK_REQUIRED", "VALIDATED"}
SKIPPED = {"SKIPPED_CUTOFF"}

BRIDGE_TAG = "synapse_full_refresh"
RESTORE_TAG = "synapse_full_refresh_restore"


def release_id(source_set_id, sequence):
    return f"{source_set_id}-{int(sequence)}"


def validate_sequence(business_date, sequence):
    """A Delta transaction version must encode its own business date and revision."""
    day = date.fromisoformat(str(business_date))
    value = int(sequence)
    if value < 0 or value // 100 != int(day.strftime("%Y%m%d")):
        raise ValueError(f"upstream_sequence {sequence} must be {day:%Y%m%d}00 through {day:%Y%m%d}99")
    return value


def txn_app_id(mapping_id):
    # Constant per table: Delta skips any write whose txnVersion is not greater
    # than the last committed one for this id. That gives retry idempotency and
    # blocks an older night from overwriting a newer one.
    return f"bridge:{mapping_id}"


def user_metadata(release, sequence, business_date, mapping_id, job_run_id, attempt_id, repair_count=None):
    # repair_count: a Databricks Repair run keeps {{job.run_id}}, so the job run
    # id alone does not identify the execution that made a commit.
    md = {"tag": BRIDGE_TAG, "release_id": release, "sequence": int(sequence),
          "business_date": str(business_date), "mapping_id": mapping_id,
          "job_run_id": str(job_run_id), "attempt_id": attempt_id}
    if repair_count is not None:
        md["repair_count"] = int(repair_count)
    return json.dumps(md, sort_keys=True, separators=(",", ":"))


def restore_metadata(release, bad_version, to_version, reason):
    # Deliberately NOT tagged BRIDGE_TAG: a restore must never look like a published night.
    return json.dumps({"tag": RESTORE_TAG, "release_id": release, "bad_version": int(bad_version),
                       "restored_to_version": int(to_version), "reason": reason},
                      sort_keys=True, separators=(",", ":"))


def parse_metadata(raw):
    if not raw:
        return None
    try:
        md = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return md if isinstance(md, dict) and md.get("tag") == BRIDGE_TAG else None


# ------------------------------------------------------------------ batching

def make_batches(mappings, max_tables, max_bytes):
    """Pack tables into batches, biggest first.

    The for-each task works through batches with fixed concurrency, so it acts
    like a work queue: putting the largest tables first stops one huge table
    from starting last and running past the cutoff. A table bigger than
    max_bytes gets a batch of its own.
    """
    ordered = sorted(mappings, key=lambda m: (-int(m.get("estimated_bytes", 0) or 0), m["mapping_id"]))
    batches, current, size = [], [], 0
    for m in ordered:
        b = int(m.get("estimated_bytes", 0) or 0)
        if current and (len(current) >= max_tables or size + b > max_bytes):
            batches.append(current)
            current, size = [], 0
        current.append(m["mapping_id"])
        size += b
    if current:
        batches.append(current)
    return batches


# -------------------------------------------------------------------- cutoff

def cutoff_utc(now_utc, local_time, tz_name):
    """Next occurrence of local_time (HH:MM) in tz_name at or after now_utc."""
    tz = ZoneInfo(tz_name)
    local_now = now_utc.astimezone(tz)
    hh, mm = (int(x) for x in local_time.split(":"))
    candidate = local_now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if candidate <= local_now:
        candidate = (local_now + timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)
    return candidate.astimezone(timezone.utc)


def business_day(now_utc, tz_name):
    """The Sydney calendar date expected by an automatic nightly run."""
    return now_utc.astimezone(ZoneInfo(tz_name)).date()


def deadline_utc(business_date, local_time, tz_name):
    """Fixed deadline for a business date; never roll it into the next day."""
    d = datetime.fromisoformat(str(business_date)).date()
    hh, mm = (int(x) for x in local_time.split(":"))
    return datetime(d.year, d.month, d.day, hh, mm,
                    tzinfo=ZoneInfo(tz_name)).astimezone(timezone.utc)


def nightly_cutoff_utc(business_date, now_utc, timezone_name, automatic_time, recovery_time, manual):
    """A same-day repair has a fixed 11:00 deadline, never tomorrow's cutoff."""
    requested = date.fromisoformat(str(business_date))
    today = business_day(now_utc, timezone_name)
    if manual and requested != today:
        raise ValueError("The nightly job only repairs today's Sydney business date; "
                         "historical backfills need a separate reviewed procedure")
    cutoff = deadline_utc(requested, recovery_time if manual else automatic_time, timezone_name)
    if now_utc >= cutoff:
        raise ValueError(f"{requested} {'recovery' if manual else 'publication'} cutoff "
                         f"{cutoff.isoformat()} has passed; no rerun is permitted")
    return cutoff


def utcnow():
    return datetime.now(timezone.utc)


def call_with_transient_retry(fn, is_transient, give_up_at, now=None, sleep=None, interval_seconds=60,
                              on_retry=None):
    """Run fn(); retry only errors is_transient() accepts, until give_up_at (aware UTC).

    Deterministic errors (a malformed manifest, wrong staging objects, a bug)
    are raised at once. A transient error is raised once the next attempt
    could not start before give_up_at, so the caller still fails loudly.
    """
    import time as _time
    now = now or utcnow
    sleep = sleep or _time.sleep
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - classified below
            if not is_transient(exc) or now() + timedelta(seconds=interval_seconds) >= give_up_at:
                raise
            if on_retry:
                on_retry(exc)
            sleep(interval_seconds)


# ------------------------------------------------------- Delta history logic
# History rows are dicts: version (int), operation (str), userMetadata (str|None),
# operationMetrics (dict[str,str]|None). Newest first, as DESCRIBE HISTORY returns.

def latest_bridge_commit(history):
    for row in history:
        md = parse_metadata(row.get("userMetadata"))
        if md is not None:
            return row, md
    return None, None


def restored_after(history, commit):
    """Whether later restore history fails to prove this exact commit remains active."""
    if commit is None:
        return False
    restores = [r for r in history if int(r["version"]) > int(commit["version"])
                and r.get("operation") == "RESTORE"]
    if not restores:
        return False
    latest = max(restores, key=lambda r: int(r["version"]))
    target = restore_target(latest)
    # A later data write after the restore means its target is no longer live.
    after = [r for r in history if int(r["version"]) > int(latest["version"])]
    return (target != int(commit["version"]) or any(
        r.get("operation") not in NON_DATA_OPERATIONS for r in after))


def dynamic_partition_overwrite(row):
    """Whether Delta recorded this write as a dynamic partition overwrite.

    Databricks Runtime 18 adds isDynamicPartitionOverwrite to WRITE
    operationParameters. A bridge publish must replace every partition, so a
    commit carrying the flag is a bad publication even when its row count
    matches: rows in partitions it did not touch are still live. Older
    runtimes omit the flag, so this can only add protection.
    """
    value = (row.get("operationParameters") or {}).get("isDynamicPartitionOverwrite")
    return str(value).strip().lower() == "true"


def restore_target(row):
    """Use recorded bridge metadata or Delta's version parameter; unknown fails closed."""
    try:
        md = json.loads(row.get("userMetadata") or "{}")
        if md.get("tag") == RESTORE_TAG:
            value = md.get("restored_to_version")
        else:
            value = (row.get("operationParameters") or {}).get("version")
        if isinstance(value, bool) or value is None or not str(value).isdigit():
            return None
        return int(value)
    except (ValueError, TypeError, AttributeError):
        return None


NON_DATA_OPERATIONS = {"OPTIMIZE", "VACUUM", "VACUUM START", "VACUUM END",
                       "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "ANALYZE"}


def intervening_change(history, commit):
    """A later commit whose effect on this release cannot be proved harmless.

    Known maintenance and table-property changes do not change rows. Governance
    and retention settings must be checked separately in deployment acceptance.
    Unknown operations fail closed. RESTORE is handled separately.
    """
    return any(int(row["version"]) > int(commit["version"])
               and row.get("operation") not in (NON_DATA_OPERATIONS | {"RESTORE"})
               for row in history)


def _newer_date_ok(md, business_date):
    if business_date is None:
        return True  # legacy callers; production passes the requested date
    try:
        return (validate_sequence(md["business_date"], md["sequence"]) == int(md["sequence"])
                and date.fromisoformat(md["business_date"]) >= date.fromisoformat(str(business_date)))
    except (KeyError, TypeError, ValueError):
        return False


def precheck(history, sequence, business_date=None):
    """Decide before exporting whether the table already has this or a newer night."""
    commit, md = latest_bridge_commit(history)
    if md is None:
        return None
    # A newer release can legitimately recover a table after a restore or an
    # out-of-band change; only block this or an older sequence.
    if md["sequence"] >= int(sequence):
        if restored_after(history, commit):
            return "FAILED_VERIFY_ROLLED_BACK"
        if intervening_change(history, commit):
            return "FAILED_VERIFY"
    if md["sequence"] == int(sequence):
        return "ALREADY_CURRENT" if (business_date is None or
                                     str(md.get("business_date")) == str(business_date)) else "FAILED_UPSTREAM_SEQUENCE"
    if md["sequence"] > int(sequence):
        return "SUPERSEDED" if _newer_date_ok(md, business_date) else "FAILED_UPSTREAM_SEQUENCE"
    return None


def verify_after_write(history, release, sequence, expected_rows, business_date=None):
    """Classify the outcome of the overwrite using the table's own history.

    Returns (status, version, detail). Status is PUBLISHED when the commit for
    this release exists with the expected row count; SUPERSEDED when Delta
    skipped the write because a newer night is already committed;
    FAILED_VERIFY_NEEDS_ROLLBACK when the commit exists with a wrong count;
    FAILED_VERIFY when a later unverified commit changes the table; and
    FAILED_WRITE when no commit can be found (safe to retry: a retry is either
    skipped by Delta or writes the same release).
    """
    latest, latest_md = latest_bridge_commit(history)
    if latest_md is not None and latest_md["sequence"] > int(sequence):
        if not _newer_date_ok(latest_md, business_date):
            return "FAILED_UPSTREAM_SEQUENCE", int(latest["version"]), "newer sequence has an inconsistent business date"
        if restored_after(history, latest):
            return "FAILED_VERIFY_ROLLED_BACK", int(latest["version"]), "newer publish was restored"
        if intervening_change(history, latest):
            return "FAILED_VERIFY", int(latest["version"]), "newer publish has a later unverified commit"
        return "SUPERSEDED", None, f"newer sequence {latest_md['sequence']} already committed"

    for row in history:
        md = parse_metadata(row.get("userMetadata"))
        if md is not None and md.get("release_id") == release:
            if business_date is not None and str(md.get("business_date")) != str(business_date):
                return "FAILED_UPSTREAM_SEQUENCE", int(row["version"]), "commit business date does not match release"
            if restored_after(history, row):
                return "FAILED_VERIFY_ROLLED_BACK", int(row["version"]), "publish was subsequently restored"
            if intervening_change(history, row):
                return "FAILED_VERIFY", int(row["version"]), "a later unverified commit changed the target"
            if dynamic_partition_overwrite(row):
                return "FAILED_VERIFY_NEEDS_ROLLBACK", int(row["version"]), \
                    "commit is a dynamic partition overwrite; untouched partitions kept stale rows"
            metrics = row.get("operationMetrics") or {}
            written = metrics.get("numOutputRows")
            written = int(written) if written not in (None, "") else None
            if written == int(expected_rows):
                return "PUBLISHED", int(row["version"]), f"{written} rows"
            return "FAILED_VERIFY_NEEDS_ROLLBACK", int(row["version"]), \
                f"commit wrote {written} rows, expected {expected_rows}"
    return "FAILED_WRITE", None, "no commit found for this release"


def release_commit(history, release):
    """Newest bridge commit for this release, with its parsed metadata (or (None, None))."""
    for row in history:
        md = parse_metadata(row.get("userMetadata"))
        if md is not None and md.get("release_id") == release:
            return row, md
    return None, None


def commit_utc(history, version):
    """Delta commit time (naive UTC) of a version, from the history's timestamp_utc; None if unknown."""
    if version is None:
        return None
    for row in history:
        if int(row["version"]) == int(version):
            raw = row.get("timestamp_utc")
            if not raw:
                return None
            try:
                value = datetime.fromisoformat(str(raw))
            except ValueError:
                return None
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            return value
    return None


def late_publications(outcomes, deadline):
    """Tables whose data landed after the deadline.

    outcomes: {mapping_id: {status, event_utc, commit_utc}} with naive UTC datetimes.
    The Delta commit time decides: the log event is written after history
    verification, so it can fall after the deadline for a commit that was live
    before it. The event time is used only when the commit time is unknown
    (rows written before v2.5), and only for PUBLISHED.
    """
    late = []
    for mapping_id, v in sorted(outcomes.items()):
        committed = v.get("commit_utc")
        if v.get("status") == "PUBLISHED":
            when = committed or v.get("event_utc")
        elif v.get("status") == "ALREADY_CURRENT":
            when = committed
        else:
            when = None
        if when is not None and when > deadline:
            late.append(mapping_id)
    return late


def rollback_target_version(history, bad_version):
    """Version to restore to when the commit at bad_version is wrong.

    Only safe when bad_version is still the newest commit; otherwise someone
    else has written since and an automatic restore could discard their work.
    """
    newest = max((int(r["version"]) for r in history), default=None)
    if newest is None or newest != int(bad_version) or int(bad_version) == 0:
        return None
    return int(bad_version) - 1


def rollback_candidates(history, logged_versions, business_date):
    """Only verified, logged publish versions that have not since been restored."""
    candidates = []
    for row in history:
        md = parse_metadata(row.get("userMetadata"))
        if (md is None or md.get("business_date") != business_date or restored_after(history, row)
                or dynamic_partition_overwrite(row)):
            continue
        version = int(row["version"])
        metrics = row.get("operationMetrics") or {}
        if (version in logged_versions and metrics.get("numOutputRows") is not None
                and int(metrics["numOutputRows"]) == int(logged_versions[version])):
            candidates.append(version)
    return candidates


# ----------------------------------------------------------------- summaries

def failure_ratio(plan_rows, latest_status):
    """Share of planned tables that did not end in a success status (0.0 when nothing is planned)."""
    if not plan_rows:
        return 0.0
    failed = sum(1 for p in plan_rows if latest_status.get(p["mapping_id"]) not in SUCCESS)
    return failed / len(plan_rows)


def summarise(plan_rows, latest_status):
    """plan_rows: [{mapping_id, mandatory}], latest_status: {mapping_id: status}."""
    counts, mandatory_failures, other_failures = {}, [], []
    for p in plan_rows:
        status = latest_status.get(p["mapping_id"], "NOT_STARTED")
        counts[status] = counts.get(status, 0) + 1
        if status not in SUCCESS:
            (mandatory_failures if p["mandatory"] else other_failures).append(
                {"mapping_id": p["mapping_id"], "status": status})
    return {"counts": dict(sorted(counts.items())), "mandatory_failures": mandatory_failures,
            "other_failures": other_failures, "ok": not mandatory_failures}
