"""Upstream row-count manifest (a CSV in Azure storage): pure logic, fully unit-tested.

Why: counting every source table in Synapse (COUNT_BIG) is a full extra scan per
table per night. The upstream already knows how many rows it wrote, so it
publishes them in a CSV and the export trusts those counts, touching Synapse only
for a table whose export disagrees with its manifest count.

Default layout (header names are configurable under manifest.columns):

    business_date,upstream_run_id,upstream_sequence,schema,table,row_count
    2026-09-25,UP-20260925,2026092500,dbo,Customer,1204331

upstream_sequence is optional; without it the sequence is yyyymmdd * 100.

Freshness is decided by the business date and run id INSIDE the file, never by
the blob's last-modified time: storage timestamps are UTC (an upstream finishing
at 00:30 Sydney time stamps "yesterday"), and a same-day partial rerun rewrites
the file with the same date.
"""
import csv
import io
import json
import re
from dataclasses import dataclass, field
from datetime import date

DEFAULT_COLUMNS = {"business_date": "business_date", "upstream_run_id": "upstream_run_id",
                   "upstream_sequence": "upstream_sequence", "schema": "schema", "table": "table",
                   "row_count": "row_count"}
REQUIRED = ("business_date", "schema", "table", "row_count")
PLACEHOLDERS = ("{business_date}", "{business_date_compact}")
COMPLETION_SOURCES = ("synapse_table", "manifest")
_ABFSS = re.compile(r"^abfss://[a-z0-9-]{3,63}@[a-z0-9]{3,24}\.dfs\.core\.windows\.net/[A-Za-z0-9_./{}=-]+$")


class ManifestError(ValueError):
    """The manifest as a whole cannot be trusted (missing columns, mixed nights, ...)."""


@dataclass
class Manifest:
    business_date: str
    upstream_run_id: str
    sequence: int
    counts: dict = field(default_factory=dict)       # mapping_id -> row count
    warnings: list = field(default_factory=list)

    def summary(self, planned):
        have = sum(1 for m in planned if m in self.counts)
        return {"tables_with_counts": have, "tables_without_counts": len(planned) - have,
                "warnings": len(self.warnings)}


MISSING_PATH_JAVA_CLASSES = ("java.io.FileNotFoundException",)
MISSING_PATH_CONDITIONS = ("PATH_NOT_FOUND", "FILE_NOT_FOUND")
_MISSING_PATH_TEXT = ("FileNotFoundException", "PATH_NOT_FOUND", "does not exist")


def java_exception_chain(exc, limit=20):
    """Java exceptions behind a Python/py4j error, outermost first (pure; works without Spark)."""
    seen, found = set(), []
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        java, depth = getattr(exc, "java_exception", None), 0
        while java is not None and depth < limit:
            found.append(java)
            depth += 1
            try:
                java = java.getCause()
            except Exception:  # noqa: BLE001 - a foreign object without getCause
                java = None
        exc = exc.__cause__ or exc.__context__
    return found


def java_class_name(java):
    try:
        return str(java.getClass().getName())
    except Exception:  # noqa: BLE001
        return ""


def error_condition(exc):
    """Spark error condition (e.g. 'PATH_NOT_FOUND'), from PySpark 4 or 3.x APIs, else ''."""
    for name in ("getCondition", "getErrorClass"):
        getter = getattr(exc, name, None)
        if callable(getter):
            try:
                value = getter()
            except Exception:  # noqa: BLE001
                value = None
            if value:
                return str(value)
    return ""


def is_missing_path_error(exc):
    """Recognize the storage not-found result without masking other read failures.

    Types and error conditions are checked before message text: Databricks
    Runtime 18 changed exception types for SQLSTATE and warns that string
    matching may break. The text match remains only as a last resort, and the
    acceptance job proves on the runtime that a missing path is recognized.
    """
    if isinstance(exc, FileNotFoundError):
        return True
    if any(c in error_condition(exc) for c in MISSING_PATH_CONDITIONS):
        return True
    if any(java_class_name(j) in MISSING_PATH_JAVA_CLASSES for j in java_exception_chain(exc)):
        return True
    message = str(exc)
    return any(s in message for s in _MISSING_PATH_TEXT)


def marker_matches(text, parsed):
    """A date-level marker is valid only for the exact CSV generation it certifies."""
    try:
        marker = json.loads(text)
        return (isinstance(marker, dict) and marker.get("business_date") == parsed.business_date
                and marker.get("upstream_run_id") == parsed.upstream_run_id
                and isinstance(marker.get("upstream_sequence"), int)
                and not isinstance(marker["upstream_sequence"], bool)
                and marker["upstream_sequence"] == parsed.sequence)
    except (TypeError, ValueError):
        return False


def read_certified(settings, business_date, mappings, read_text, is_missing):
    """Read a manifest and its marker after readiness check; tolerate removal races."""
    try:
        csv_text = read_text(path_for(settings, business_date))
        marker_text = read_text(marker_path(settings, business_date))
    except Exception as exc:
        if is_missing(exc):
            return None, "manifest or marker disappeared while reading"
        raise
    # An existing marker with a malformed CSV is a broken upstream contract.
    parsed = parse(csv_text, settings, mappings, expected_business_date=str(business_date), require_run_id=True)
    if not marker_matches(marker_text, parsed):
        return None, "manifest marker does not certify the CSV"
    return parsed, None


# ------------------------------------------------------------------ settings

def optional_counts(settings, business_date, upstream_run_id, mappings, ready, read_text):
    """Manifest counts in synapse_table completion mode: optional and never blocking.

    The Synapse completion row is the completion signal in this mode, so any
    manifest problem (absent, unreadable, access denied, wrong encoding, too
    large, disappearing mid-read, malformed or for another run) only means
    the tables are counted in Synapse instead. Returns (Manifest or None, note).
    """
    try:
        if not ready(business_date):
            return None, f"no manifest for {business_date}; counting in Synapse"
        parsed = parse(read_text(path_for(settings, business_date)), settings, mappings,
                       expected_business_date=str(business_date), expected_run_id=upstream_run_id)
        return parsed, None
    except Exception as exc:   # noqa: BLE001 - deliberately non-blocking in this mode
        return None, f"manifest ignored ({type(exc).__name__}: {str(exc)[:500]}); counting in Synapse"


def completion_source(settings):
    return settings.get("upstream", {}).get("completion_source", "synapse_table")


def enabled(settings):
    return bool(settings.get("manifest"))


def columns(settings):
    return {**DEFAULT_COLUMNS, **settings.get("manifest", {}).get("columns", {})}


def validate_settings(settings, problems):
    """Append problems for the upstream/manifest settings (called by validate_inventory)."""
    src = completion_source(settings)
    if src not in COMPLETION_SOURCES:
        problems.append(f"upstream.completion_source must be one of {COMPLETION_SOURCES}")
    mf = settings.get("manifest")
    if mf is None:
        if src == "manifest":
            problems.append("upstream.completion_source is 'manifest' but no manifest settings are given")
        return
    if not isinstance(mf, dict):
        problems.append("manifest must be an object")
        return
    tpl = str(mf.get("path_template", ""))
    if not _ABFSS.fullmatch(tpl) or sum(tpl.count(p) for p in PLACEHOLDERS) != 1 or ".." in tpl:
        problems.append("manifest.path_template must be an abfss:// file path containing exactly one of "
                        "{business_date} or {business_date_compact}")
    if tpl.rsplit("/", 1)[-1].startswith(("_", ".")):
        problems.append("manifest.path_template file name must not start with '_' or '.' "
                        "(Spark file sources ignore such names)")
    if tpl.startswith(str(settings.get("staging", {}).get("root", "")).rstrip("/") + "/"):
        problems.append("manifest.path_template must use a separate read-only container from staging.root")
    marker = mf.get("success_marker")
    if marker is not None and not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", str(marker)):
        problems.append("manifest.success_marker must be a simple file name or null")
    if src == "manifest" and marker is None:
        problems.append("manifest.success_marker is required when the manifest is the completion signal "
                        "(the upstream writes it last, after the CSV is complete)")
    cols = mf.get("columns", {})
    if not isinstance(cols, dict) or any(k not in DEFAULT_COLUMNS for k in cols):
        problems.append(f"manifest.columns may only rename {sorted(DEFAULT_COLUMNS)}")
    elif len(set(columns(settings).values())) != len(DEFAULT_COLUMNS):
        problems.append("manifest.columns must map to distinct header names")
    if src == "manifest" and not columns(settings).get("upstream_run_id"):
        problems.append("manifest.columns.upstream_run_id is required when the manifest is the completion signal")
    if len(str(mf.get("delimiter", ","))) != 1:
        problems.append("manifest.delimiter must be a single character")


def path_for(settings, business_date):
    d = business_date if isinstance(business_date, date) else date.fromisoformat(str(business_date))
    tpl = settings["manifest"]["path_template"]
    return tpl.replace("{business_date}", d.isoformat()).replace("{business_date_compact}", d.strftime("%Y%m%d"))


def marker_path(settings, business_date):
    marker = settings["manifest"].get("success_marker")
    if not marker:
        return None
    return path_for(settings, business_date).rsplit("/", 1)[0] + "/" + marker


def derived_sequence(business_date):
    return int(date.fromisoformat(str(business_date)).strftime("%Y%m%d")) * 100


# ------------------------------------------------------------------- parsing

def parse(text, settings, mappings, expected_business_date=None, expected_run_id=None, require_run_id=False):
    """Parse and validate the manifest text.

    Whole-file problems raise ManifestError (do not trust any count).
    Row problems only drop that table's count, with a warning, so the table
    falls back to a Synapse count; a bad row can never block a good table.
    """
    cols = columns(settings)
    delimiter = settings["manifest"].get("delimiter", ",")
    text = text.lstrip("\ufeff")                          # BOM written by some tools
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    headers = [h.strip() for h in (reader.fieldnames or [])]
    reader.fieldnames = headers
    missing = [cols[k] for k in REQUIRED if cols[k] not in headers]
    if missing:
        raise ManifestError(f"manifest is missing column(s) {missing}; found {headers}")
    has_run, has_seq = cols["upstream_run_id"] in headers, cols["upstream_sequence"] in headers
    rows = [{k: (v or "").strip() for k, v in r.items() if k is not None} for r in reader]
    rows = [r for r in rows if any(r.values())]           # ignore blank lines
    if not rows:
        raise ManifestError("manifest has no data rows")

    dates = {r[cols["business_date"]] for r in rows}
    if len(dates) != 1:
        raise ManifestError(f"manifest mixes business dates {sorted(dates)}")
    business_date = dates.pop()
    try:
        business_date = date.fromisoformat(business_date).isoformat()
    except ValueError:
        raise ManifestError(f"manifest business_date {business_date!r} is not yyyy-mm-dd") from None
    if expected_business_date and business_date != str(expected_business_date):
        raise ManifestError(f"manifest is for {business_date}, expected {expected_business_date}")

    run_id = ""
    if has_run:
        runs = {r[cols["upstream_run_id"]] for r in rows}
        if len(runs) != 1 or not next(iter(runs)):
            raise ManifestError(f"manifest must have one non-empty upstream_run_id, found {sorted(runs)}")
        run_id = runs.pop()
    if require_run_id and not has_run:
        raise ManifestError(f"manifest has no {cols['upstream_run_id']!r} column, which is required when it is "
                            "the completion signal")
    if expected_run_id and has_run and run_id != expected_run_id:
        raise ManifestError(f"manifest is for upstream run {run_id}, expected {expected_run_id}")

    sequence = derived_sequence(business_date)
    if has_seq:
        seqs = {r[cols["upstream_sequence"]] for r in rows}
        if len(seqs) != 1:
            raise ManifestError(f"manifest mixes upstream sequences {sorted(seqs)}")
        raw = seqs.pop()
        if not re.fullmatch(r"[0-9]+", raw) or int(raw) // 100 != sequence // 100:
            raise ManifestError(f"upstream_sequence {raw!r} must be yyyymmdd followed by a 2-digit revision "
                                f"for {business_date} (e.g. {sequence} or {sequence + 1})")
        sequence = int(raw)

    by_object = {}
    for m in mappings:
        by_object.setdefault((m["source_schema"].lower(), m["source_object"].lower()), []).append(m["mapping_id"])
    counts, conflicts, warnings, unknown = {}, set(), [], 0
    for r in rows:
        key = (r[cols["schema"]].lower(), r[cols["table"]].lower())
        mapping_ids = by_object.get(key)
        if mapping_ids is None:
            unknown += 1
            continue
        raw = r[cols["row_count"]]
        if not re.fullmatch(r"[0-9]+", raw):
            warnings.append(f"{key[0]}.{key[1]}: row_count {raw!r} is not a whole number; will count in Synapse")
            conflicts.update(mapping_ids)
            continue
        n = int(raw)
        for mid in mapping_ids:
            if mid in counts and counts[mid] != n:
                warnings.append(f"{key[0]}.{key[1]}: conflicting counts {counts[mid]} and {n}; will count in Synapse")
                conflicts.add(mid)
            counts.setdefault(mid, n)
    for mid in conflicts:
        counts.pop(mid, None)
    if unknown:
        warnings.append(f"{unknown} manifest row(s) name objects that are not in the inventory (ignored)")
    return Manifest(business_date=business_date, upstream_run_id=run_id, sequence=sequence,
                    counts=counts, warnings=warnings)
