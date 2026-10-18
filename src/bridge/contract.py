"""Inventory contract: type mapping, validation and SQL text builders.

Everything that ends up inside SQL text is derived here from validated metadata.
The inventory stores source column metadata (name, SQL type, length, precision,
scale); export expressions and target types are recomputed at runtime, so a
hand-edited inventory cannot inject SQL.
"""
import hashlib
import json
import re

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
KEY = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$")
TIME_OF_DAY = re.compile(r"^([01][0-9]|2[0-3]):[0-5][0-9]$")
SERVER = re.compile(r"^[a-z0-9-]{1,63}\.(?:sql\.azuresynapse|database\.windows)\.net$")
ABFSS_ROOT = re.compile(r"^abfss://[a-z0-9-]{3,63}@[a-z0-9]{3,24}\.dfs\.core\.windows\.net$")
REL_PATH = re.compile(r"^[A-Za-z0-9_./=-]{1,900}$")
METADATA_PREFIX = "_bridge_"
METADATA_COLUMNS = (("_bridge_business_date", "date"), ("_bridge_release_id", "string"))
SCHEMA_HASH_ALGORITHM = "sys-columns-v1"

# Spark can read CETAS timestamps as either type, depending on the Parquet encoding
# the pool writes; both carry the same wall-clock value once the session is UTC.
PARQUET_ACCEPT = {"timestamp": {"timestamp", "timestamp_ntz"}}

TEXT_TYPES = {"char", "varchar", "nchar", "nvarchar"}
DATETIME_TYPES = {"datetime", "datetime2", "smalldatetime"}
DEFAULT_EXECUTION = {
    "history_limit": 200,             # Delta history rows scanned for bridge commits
    "max_upstream_lag_days": 0,       # deprecated: automatic runs require today's Sydney date
    "upstream_wait_minutes": 0,       # plan polls this long for tonight's COMPLETE row
    "jdbc_fallback_enabled": True,    # hand CETAS-specific failures to the serverless UC-JDBC task
    "jdbc_fetch_size": 10000,
    "jdbc_num_partitions": 4,         # parallel reads when a mapping sets jdbc_partition_column
    "max_jdbc_fallbacks_per_batch": 2,
    "fallback_alert_threshold": 10,
    "additional_access_error_codes": (),   # confirmed access-denied codes (see synapse.ACCESS_ERROR_CANDIDATES)
    "recovery_cutoff_local_time": "11:00",
    "publish_margin_minutes": 5,
    "max_failed_table_ratio": 0.02,   # finalize fails when more planned tables than this did not succeed
    "plan_transient_retry_minutes": 15,   # plan keeps retrying transient Synapse/storage errors this long
}
MIN_GOVERNANCE_RUNTIME_MAJOR = 17     # DESCRIBE ... AS JSON exposes row_filter/column_masks from DBR 17.0
POLICY_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,255}$")
PARTITIONABLE_SQL_TYPES = {"tinyint", "smallint", "int", "bigint", "date"}

DEFAULT_TABLE_PROPERTIES = {
    "delta.deletedFileRetentionDuration": "interval 7 days",
    "delta.logRetentionDuration": "interval 30 days",
}


class ContractError(ValueError):
    """A deterministic problem: retrying the same input cannot succeed."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class AncientCetasTimestamp(ContractError):
    """A pre-1900 CETAS timestamp needs the Spark-written JDBC route."""


class UnsupportedType(ValueError):
    pass


def ident(value, what="identifier"):
    if not isinstance(value, str) or not IDENT.fullmatch(value):
        raise ValueError(f"Unsafe {what}: {value!r}")
    return value


def q_sql(name):
    """Bracket-quote a validated T-SQL identifier."""
    return f"[{ident(name)}]"


def q_spark(name):
    """Backtick-quote a validated Spark identifier."""
    return f"`{ident(name)}`"


def fq_target(m):
    return ".".join(q_spark(m[k]) for k in ("target_catalog", "target_schema", "target_table"))


def fq_control(settings, table):
    c = settings["control"]
    return f"{q_spark(c['catalog'])}.{q_spark(c['schema'])}.{q_spark(table)}"


def column_contract(col):
    """Map one source column to (export expression, Parquet type, target type).

    Casts happen inside the CETAS SELECT so the Parquet types are fixed and
    predictable regardless of how the pool would encode the raw type.
    """
    name = ident(col["name"], "column name")
    t = str(col["sql_type"]).lower()
    src = q_sql(name)
    p, s = col.get("precision"), col.get("scale")

    def out(expr, parquet, target):
        return {"name": name, "export_expr": expr, "parquet_type": parquet, "target_type": target}

    if t in TEXT_TYPES:
        if col.get("max_length") == -1:
            raise UnsupportedType(f"{name}: {t}(max) needs a reviewed design (LOB export)")
        return out(src, "string", "string")
    if t == "bit":
        return out(src, "boolean", "boolean")
    if t in ("tinyint", "smallint"):
        return out(f"CAST({src} AS int)", "int", "int")
    if t == "int":
        return out(src, "int", "int")
    if t == "bigint":
        return out(src, "bigint", "bigint")
    if t in ("decimal", "numeric"):
        if not (isinstance(p, int) and isinstance(s, int) and 1 <= p <= 38 and 0 <= s <= p):
            raise UnsupportedType(f"{name}: invalid decimal precision/scale {p},{s}")
        return out(f"CAST({src} AS decimal({p},{s}))", f"decimal({p},{s})", f"decimal({p},{s})")
    if t == "money":
        return out(f"CAST({src} AS decimal(19,4))", "decimal(19,4)", "decimal(19,4)")
    if t == "smallmoney":
        return out(f"CAST({src} AS decimal(10,4))", "decimal(10,4)", "decimal(10,4)")
    if t == "float":
        return out(f"CAST({src} AS float)", "double", "double")
    if t == "real":
        return out(f"CAST({src} AS float)", "double", "double")
    if t == "date":
        return out(src, "date", "date")
    if t in DATETIME_TYPES:
        # SQL Server datetimes have no time zone: keep the wall clock (timestamp_ntz).
        return out(f"CAST({_to_micros(src, t, s)} AS datetime2(6))", "timestamp", "timestamp_ntz")
    if t == "datetimeoffset":
        # Normalise to UTC; the value is a real instant, so the target is timestamp.
        return out(f"CAST({_to_micros(f'SWITCHOFFSET({src}, {chr(39)}+00:00{chr(39)})', t, s)} AS datetime2(6))",
                   "timestamp", "timestamp")
    if t == "time":
        return out(f"CAST({src} AS varchar(16))", "string", "string")
    if t == "uniqueidentifier":
        return out(f"CAST({src} AS char(36))", "string", "string")
    raise UnsupportedType(f"{name}: SQL type {t!r} is not supported by the automatic contract")


def _to_micros(expr, sql_type, scale):
    """Drop the 7th fractional digit before casting to datetime2(6).

    CAST rounds, so datetime2(7) '9999-12-31 23:59:59.9999999' (a common
    end-of-time sentinel) would round up past the maximum and fail the whole
    CETAS. Truncating first makes the cast exact. Spark stores microseconds, so
    nothing representable is lost. Only scale-7 types need it.
    """
    if sql_type in ("datetime2", "datetimeoffset") and scale == 7:
        return f"DATEADD(nanosecond, -(DATEPART(nanosecond, {expr}) % 1000), {expr})"
    return expr


EXEMPTION_KEYS = ("catalog", "schema", "table", "name")


def _policy_key(catalog, schema, table, name):
    """Identity of one ABAC policy: the securable it is defined on plus its name.

    Policy names are unique only per securable, so a name alone can match an
    unreviewed policy of the same name on another catalog, schema or table.
    Unity Catalog names are case-insensitive.
    """
    norm = lambda v: None if v in (None, "") else str(v).strip().lower()
    return (norm(catalog), norm(schema), norm(table), norm(name))


def exemption_problems(entries):
    """Validation messages for governance.exempt_abac_policies (pure)."""
    if not isinstance(entries, list):
        return ["governance.exempt_abac_policies must be a list"]
    problems = []
    for i, e in enumerate(entries):
        where = f"governance.exempt_abac_policies[{i}]"
        if not isinstance(e, dict):
            problems.append(f"{where} must be an object {{catalog, schema?, table?, name}} naming the securable "
                            "the policy is defined on (a bare policy name is ambiguous across securables)")
            continue
        unknown = sorted(set(e) - set(EXEMPTION_KEYS))
        if unknown:
            problems.append(f"{where} has unknown keys {unknown}")
        if not (isinstance(e.get("name"), str) and POLICY_NAME.fullmatch(e["name"])):
            problems.append(f"{where}.name must be a policy name")
        if not (isinstance(e.get("catalog"), str) and IDENT.fullmatch(e["catalog"])):
            problems.append(f"{where}.catalog must be an identifier")
        for k in ("schema", "table"):
            if e.get(k) is not None and not (isinstance(e[k], str) and IDENT.fullmatch(e[k])):
                problems.append(f"{where}.{k} must be an identifier or null")
        if e.get("table") is not None and e.get("schema") is None:
            problems.append(f"{where}: a table-level policy also needs its schema")
    return problems


def exempt_abac_policies(settings):
    """ABAC policies reviewed as NOT applying to the job and rollback identities.

    Each entry is a human decision about ONE policy, identified by the securable
    it is defined on and its name: its EXCEPT clause lists both identities
    (confirm with DESCRIBE POLICY). Table-level row filters and column masks can
    never be exempted; they have no EXCEPT clause.
    """
    entries = (settings.get("governance") or {}).get("exempt_abac_policies", [])
    problems = exemption_problems(entries)
    if problems:
        raise ValueError("; ".join(problems))
    return {_policy_key(e["catalog"], e.get("schema"), e.get("table"), e["name"]) for e in entries}


def _policy_label(kind, key):
    scope = ".".join(v for v in key[:3] if v is not None) or "?"
    return f"{kind}:{scope}/{key[3]}"


def governance_problems(policy_rows, describe_json, exempt=()):
    """Pure decision for the target governance preflight.

    policy_rows: SHOW EFFECTIVE POLICIES rows as dicts ("Policy Name", "Policy Type",
    "Catalog", "Schema", "Table").
    describe_json: the DESCRIBE TABLE EXTENDED ... AS JSON text.
    exempt: policy keys from exempt_abac_policies.
    Returns (blocking, exempted). Raises ValueError when the metadata cannot be trusted.

    SHOW EFFECTIVE POLICIES also lists policies inherited from the catalog and
    schema, whether or not their tag conditions match this table or exempt the
    job identity. Only reviewed policies in governance.exempt_abac_policies,
    matched by defining securable AND name, are set aside; everything else still
    blocks. Table-level filters and masks (the JSON row_filter / column_masks
    keys) always block.
    """
    exempt = set(exempt or ())
    blocking, exempted = [], []
    for row in policy_rows:
        kind = str(row.get("Policy Type", "")).upper().replace(" ", "_")
        if kind not in ("ROW_FILTER", "COLUMN_MASK"):
            continue
        key = _policy_key(row.get("Catalog"), row.get("Schema"), row.get("Table"), row.get("Policy Name"))
        # A row without its defining catalog cannot be matched to a review.
        matched = key[0] is not None and key[3] is not None and key in exempt
        (exempted if matched else blocking).append(_policy_label(kind, key))
    details = json.loads(describe_json)
    if not isinstance(details, dict) or "columns" not in details:
        raise ValueError("invalid DESCRIBE JSON metadata")
    if details.get("row_filter"):
        blocking.append("table-level row_filter")
    if details.get("column_masks"):
        blocking.append("table-level column_masks")
    return blocking, exempted


def runtime_major(spark_version):
    """Major Databricks Runtime version from a selector such as '18.x-scala2.13' or '17.3.x-scala2.13'."""
    match = re.match(r"^(?:custom:.*?__)?(\d+)\.", str(spark_version or ""))
    return int(match.group(1)) if match else None


def execution(settings, key):
    return settings["execution"].get(key, DEFAULT_EXECUTION[key])


def columns(m):
    return [column_contract(c) for c in m["columns"]]


def projection_sql(m):
    return ", ".join(f"{c['export_expr']} AS {q_sql(c['name'])}" for c in columns(m))


def target_schema(m):
    """Expected target columns (lower-case name -> Spark type), including metadata."""
    out = {c["name"].lower(): c["target_type"] for c in columns(m)}
    out.update({n: t for n, t in METADATA_COLUMNS})
    return out


def schema_hash(rows):
    """Hash sys.columns rows: (name, column_id, type, max_length, precision, scale, is_nullable)."""
    body = json.dumps([[str(r[0]), int(r[1]), str(r[2]).lower(), int(r[3]), int(r[4]), int(r[5]), bool(int(r[6]))]
                       for r in rows], separators=(",", ":"))
    return "sha256:" + hashlib.sha256(f"{SCHEMA_HASH_ALGORITHM}\n{body}".encode()).hexdigest()


def columns_from_sys_rows(rows):
    return [{"name": str(r[0]), "sql_type": str(r[2]).lower(), "max_length": int(r[3]),
             "precision": int(r[4]), "scale": int(r[5]), "nullable": bool(int(r[6]))} for r in rows]


# ----------------------------------------------------------------- SQL text

SYS_COLUMNS_SQL = (
    "SELECT c.name, c.column_id, t.name, c.max_length, c.precision, c.scale, "
    "CAST(c.is_nullable AS int) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id "
    "WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id")


ALL_COLUMNS_SQL = (
    "SELECT s.name, o.name, c.name, c.column_id, t.name, c.max_length, c.precision, c.scale, "
    "CAST(c.is_nullable AS int) FROM sys.columns c "
    "JOIN sys.objects o ON o.object_id=c.object_id JOIN sys.schemas s ON s.schema_id=o.schema_id "
    "JOIN sys.types t ON c.user_type_id=t.user_type_id "
    "WHERE o.type IN ('U','V') AND s.name IN ({params}) ORDER BY s.name, o.name, c.column_id")


def all_columns_sql(schema_count):
    return ALL_COLUMNS_SQL.format(params=",".join("?" * int(schema_count)))


def hashes_from_rows(rows, mappings):
    """Schema hash per mapping from ONE catalog query over every source schema.

    rows: (schema, object, name, column_id, type, max_length, precision, scale, nullable).
    The per-object rows are exactly what SYS_COLUMNS_SQL returns, so the hash is
    identical to the one the generator stored. Missing objects map to None.
    """
    grouped = {}
    for r in rows:
        grouped.setdefault((str(r[0]).lower(), str(r[1]).lower()), []).append(tuple(r[2:]))
    out = {}
    for m in mappings:
        cols = grouped.get((m["source_schema"].lower(), m["source_object"].lower()))
        out[m["mapping_id"]] = schema_hash(sorted(cols, key=lambda c: int(c[1]))) if cols else None
    return out


def object_name(m):
    return f"{q_sql(m['source_schema'])}.{q_sql(m['source_object'])}"


def count_sql(m):
    # Named column: a Spark JDBC "query" wraps this in a derived table, and T-SQL
    # rejects an unnamed derived-table column (error 8155).
    return f"SELECT COUNT_BIG(*) AS row_count FROM {object_name(m)}"


def cetas_sql(m, settings, external_table, location):
    st = settings["staging"]
    if not REL_PATH.fullmatch(location) or ".." in location or not location.endswith("/"):
        raise ValueError(f"Unsafe staging location: {location!r}")
    return (f"CREATE EXTERNAL TABLE {q_sql(st['metadata_schema'])}.{q_sql(external_table)} "
            f"WITH (LOCATION = '{location}', DATA_SOURCE = {q_sql(st['external_data_source'])}, "
            f"FILE_FORMAT = {q_sql(st['file_format'])}) "
            f"AS SELECT {projection_sql(m)} FROM {object_name(m)}")


def jdbc_select_sql(m):
    """The fallback SELECT in native Synapse SQL dialect."""
    return f"SELECT {projection_sql(m)} FROM {object_name(m)}"


def partition_column(m):
    """The validated partition column for parallel JDBC reads, or None."""
    name = m.get("jdbc_partition_column")
    if not name:
        return None
    col = next((c for c in m["columns"] if c["name"] == name), None)
    if col is None or str(col["sql_type"]).lower() not in PARTITIONABLE_SQL_TYPES:
        raise ValueError(f"jdbc_partition_column {name!r} must be an integer or date column of the mapping")
    return col


def bounds_sql(m):
    col = partition_column(m)
    src = q_sql(col["name"])
    if str(col["sql_type"]).lower() == "date":
        lo, hi = f"CONVERT(char(10), MIN({src}), 23)", f"CONVERT(char(10), MAX({src}), 23)"
    else:
        lo, hi = f"CAST(MIN({src}) AS varchar(40))", f"CAST(MAX({src}) AS varchar(40))"
    # Named columns: through a Spark/UC JDBC "query" this becomes a derived table,
    # and T-SQL rejects unnamed derived-table columns (error 8155).
    return f"SELECT {lo} AS lo, {hi} AS hi FROM {object_name(m)}"


def drop_external_sql(settings, external_table):
    schema = settings["staging"]["metadata_schema"]
    name = f"{q_sql(schema)}.{q_sql(external_table)}"
    return f"IF OBJECT_ID(N'{name}') IS NOT NULL DROP EXTERNAL TABLE {name}"


# --------------------------------------------------------------- validation

def _need(problems, cond, message):
    if not cond:
        problems.append(message)


def validate_inventory(doc):
    """Validate the whole inventory; raise one ValueError listing every problem."""
    problems = []
    need = lambda cond, msg: _need(problems, cond, msg)
    for k in ("inventory_version", "source_set_id", "synapse", "staging", "control", "execution", "mappings"):
        need(k in doc, f"missing top-level key {k!r}")
    if problems:
        raise ValueError("Invalid inventory:\n  " + "\n  ".join(problems))

    need(bool(KEY.fullmatch(str(doc["inventory_version"]))), "inventory_version must match [A-Za-z0-9_-]")
    need(bool(KEY.fullmatch(str(doc["source_set_id"]))), "source_set_id must match [A-Za-z0-9_-]")
    syn = doc["synapse"]
    need(bool(SERVER.fullmatch(str(syn.get("server", "")))), "synapse.server must be <workspace>.sql.azuresynapse.net or <server>.database.windows.net")
    need(bool(IDENT.fullmatch(str(syn.get("database", "")))), "synapse.database must be an identifier")
    auth = syn.get("auth", {})
    need(auth.get("mode") in ("service_credential", "client_secret", "sql_password"),
         "synapse.auth.mode must be service_credential, client_secret or sql_password")
    if auth.get("mode") == "service_credential":
        need(bool(auth.get("credential_name")), "synapse.auth.credential_name is required")
    if auth.get("mode") == "client_secret":
        for k in ("secret_scope", "tenant_id_key", "client_id_key", "client_secret_key"):
            need(bool(auth.get(k)), f"synapse.auth.{k} is required")
    if auth.get("mode") == "sql_password":
        for k in ("secret_scope", "user_key", "password_key"):
            need(bool(auth.get(k)), f"synapse.auth.{k} is required")
    # The completion table is read only in synapse_table mode (the default when unset).
    if (doc.get("upstream", {}).get("completion_source", "synapse_table") == "synapse_table"
            or "upstream_control_schema" in syn):
        need(IDENT.fullmatch(str(syn.get("upstream_control_schema", ""))) is not None,
             "synapse.upstream_control_schema must be an identifier")
    if doc.get("execution", {}).get("jdbc_fallback_enabled", True) or syn.get("uc_jdbc_connection") is not None:
        need(IDENT.fullmatch(str(syn.get("uc_jdbc_connection", ""))) is not None,
             "synapse.uc_jdbc_connection must be an identifier when execution.jdbc_fallback_enabled=true")

    st = doc["staging"]
    need(bool(ABFSS_ROOT.fullmatch(str(st.get("root", "")))),
         "staging.root must be abfss://<container>@<account>.dfs.core.windows.net")
    need(bool(re.fullmatch(r"[A-Za-z0-9_-]{1,64}", str(st.get("prefix", "")))), "staging.prefix must be a simple folder name")
    for k in ("metadata_schema", "external_data_source", "file_format"):
        need(IDENT.fullmatch(str(st.get(k, ""))) is not None, f"staging.{k} must be an identifier")

    from . import manifest as _manifest   # local import: manifest has no dependency on contract
    _manifest.validate_settings(doc, problems)

    ctl = doc["control"]
    for k in ("catalog", "schema"):
        need(IDENT.fullmatch(str(ctl.get(k, ""))) is not None, f"control.{k} must be an identifier")

    notifications = doc.get("notifications")
    need(isinstance(notifications, dict), "notifications must be an object")
    webhook = notifications.get("summary_webhook") if isinstance(notifications, dict) else None
    need(isinstance(webhook, dict), "notifications.summary_webhook must be an object")
    if isinstance(webhook, dict):
        for k in ("secret_scope", "url_secret_key"):
            need(bool(KEY.fullmatch(str(webhook.get(k, "")))),
                 f"notifications.summary_webhook.{k} must be a secret scope/key name")

    ex = doc["execution"]
    for k, lo, hi in (("max_tables_per_batch", 1, 200), ("export_timeout_seconds", 60, 6 * 3600)):
        v = ex.get(k)
        need(isinstance(v, int) and not isinstance(v, bool) and lo <= v <= hi, f"execution.{k} must be {lo}..{hi}")
    need(isinstance(ex.get("max_bytes_per_batch"), int) and ex.get("max_bytes_per_batch", 0) > 0,
         "execution.max_bytes_per_batch must be a positive integer")
    need(bool(TIME_OF_DAY.fullmatch(str(ex.get("cutoff_local_time", "")))), "execution.cutoff_local_time must be HH:MM")
    recovery_time = ex.get("recovery_cutoff_local_time", DEFAULT_EXECUTION["recovery_cutoff_local_time"])
    need(bool(TIME_OF_DAY.fullmatch(str(recovery_time))), "execution.recovery_cutoff_local_time must be HH:MM")
    if TIME_OF_DAY.fullmatch(str(ex.get("cutoff_local_time", ""))) and TIME_OF_DAY.fullmatch(str(recovery_time)):
        need(recovery_time > ex["cutoff_local_time"], "recovery cutoff must follow the automatic publication cutoff")
    margin = ex.get("publish_margin_minutes", DEFAULT_EXECUTION["publish_margin_minutes"])
    need(isinstance(margin, int) and not isinstance(margin, bool) and 0 <= margin <= 60,
         "execution.publish_margin_minutes must be 0..60")
    ratio = ex.get("max_failed_table_ratio", DEFAULT_EXECUTION["max_failed_table_ratio"])
    need(isinstance(ratio, (int, float)) and not isinstance(ratio, bool) and 0 <= ratio <= 1,
         "execution.max_failed_table_ratio must be a number 0..1")
    retry_minutes = ex.get("plan_transient_retry_minutes", DEFAULT_EXECUTION["plan_transient_retry_minutes"])
    need(isinstance(retry_minutes, int) and not isinstance(retry_minutes, bool) and 0 <= retry_minutes <= 120,
         "execution.plan_transient_retry_minutes must be 0..120")
    gov = doc.get("governance", {})
    if not isinstance(gov, dict):
        problems.append("governance must be an object")
    else:
        problems.extend(exemption_problems(gov.get("exempt_abac_policies", [])))
    need(bool(ex.get("cutoff_timezone")), "execution.cutoff_timezone is required (e.g. Australia/Sydney)")
    if ex.get("cutoff_timezone"):
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo(ex["cutoff_timezone"])
        except Exception:
            problems.append(f"execution.cutoff_timezone {ex['cutoff_timezone']!r} is not a known time zone")
    for k, lo, hi in (("history_limit", 50, 1000), ("max_upstream_lag_days", 0, 7),
                      ("upstream_wait_minutes", 0, 600), ("jdbc_fetch_size", 100, 100000),
                      ("jdbc_num_partitions", 1, 64), ("max_jdbc_fallbacks_per_batch", 0, 200),
                      ("fallback_alert_threshold", 0, 100000)):
        v = ex.get(k, DEFAULT_EXECUTION[k])
        need(isinstance(v, int) and not isinstance(v, bool) and lo <= v <= hi, f"execution.{k} must be {lo}..{hi}")
    need(isinstance(ex.get("jdbc_fallback_enabled", True), bool), "execution.jdbc_fallback_enabled must be true/false")
    extra_access = ex.get("additional_access_error_codes", [])
    if not isinstance(extra_access, (list, tuple)) or not all(
            isinstance(c, int) and not isinstance(c, bool) and 0 < c < 2 ** 31 for c in extra_access):
        problems.append("execution.additional_access_error_codes must be a list of positive SQL error codes")
    else:
        from . import synapse as _synapse   # local import: synapse imports contract
        taken = (set(_synapse.SCHEMA_ERROR_CODES) | set(_synapse.DATA_ERROR_CODES)
                 | set(_synapse.CAPACITY_ERROR_CODES))
        clash = sorted(set(extra_access) & taken)
        need(not clash, f"execution.additional_access_error_codes {clash} are already schema, data or capacity codes")

    props = doc.get("table_properties", DEFAULT_TABLE_PROPERTIES)
    need(isinstance(props, dict), "table_properties must be an object")
    for k, v in (props.items() if isinstance(props, dict) else []):
        need(str(k).lower() != "delta.settransactionretentionduration",
             "delta.setTransactionRetentionDuration can expire the overwrite idempotency guard")
        need(bool(re.fullmatch(r"[A-Za-z0-9_.]{1,128}", str(k))) and "'" not in str(v) and "\\" not in str(v),
             f"unsafe table property {k!r}")

    ms = doc["mappings"]
    need(isinstance(ms, list) and ms, "mappings must be a non-empty list")
    ids, targets, sources, external_prefixes = set(), set(), {}, set()
    for i, m in enumerate(ms if isinstance(ms, list) else []):
        where = f"mappings[{i}]"
        mid = m.get("mapping_id")
        need(bool(KEY.fullmatch(str(mid))), f"{where}.mapping_id must match [A-Za-z0-9_-]")
        need(mid not in ids, f"duplicate mapping_id {mid!r}")
        ids.add(mid)
        # CETAS external names use the first 80 normalized characters. Two
        # mapping IDs must never claim each other's orphaned external table.
        external_prefix = str(mid).replace("-", "_")[:80].lower()
        need(external_prefix not in external_prefixes,
             f"mapping_id {mid!r} collides with another external-table name prefix")
        external_prefixes.add(external_prefix)
        where = f"mapping {mid}"
        for k in ("source_schema", "source_object", "target_catalog", "target_schema", "target_table"):
            need(IDENT.fullmatch(str(m.get(k, ""))) is not None, f"{where}: {k} must be an identifier")
        source = (str(m.get("source_schema", "")), str(m.get("source_object", "")))
        folded = (source[0].lower(), source[1].lower())
        need(folded not in sources or sources[folded] == source,
             f"{where}: source {source} differs only in case from {sources.get(folded)}")
        sources[folded] = source
        key = ".".join(str(m.get(k, "")).lower() for k in ("target_catalog", "target_schema", "target_table"))
        need(key not in targets, f"{where}: target {key} is used by more than one mapping")
        targets.add(key)
        for k in ("enabled", "mandatory", "allow_empty", "verify_source_count"):
            need(isinstance(m.get(k, False), bool), f"{where}: {k} must be true/false")
        need(str(m.get("source_schema_hash", "")).startswith("sha256:"), f"{where}: source_schema_hash is required")
        eb = m.get("estimated_bytes", 0)
        need(isinstance(eb, int) and not isinstance(eb, bool) and eb >= 0, f"{where}: estimated_bytes must be >= 0")
        cols = m.get("columns")
        need(isinstance(cols, list) and cols, f"{where}: columns must be a non-empty list")
        seen = set()
        for c in cols if isinstance(cols, list) else []:
            try:
                column_contract(c)
            except (ValueError, KeyError) as e:
                problems.append(f"{where}: {e}")
                continue
            n = c["name"].lower()
            need(n not in seen, f"{where}: duplicate column {c['name']!r} (names are case-insensitive in Spark)")
            need(not n.startswith(METADATA_PREFIX), f"{where}: column {c['name']!r} uses the reserved prefix _bridge_")
            seen.add(n)
        if m.get("jdbc_partition_column") and isinstance(cols, list):
            try:
                partition_column(m)
            except (ValueError, KeyError) as e:
                problems.append(f"{where}: {e}")
    if problems:
        raise ValueError("Invalid inventory:\n  " + "\n  ".join(problems))
    return doc


def variant_4a_problems(doc):
    """Settings this serverless variant (4A) needs, checked before deploy and UC setup.

    The JDBC fallback reads Synapse only through a Unity Catalog JDBC connection, and
    the upstream signals completion with the manifest. Without these the gap would
    first show at night, after CETAS had already failed.
    """
    problems = []
    if not IDENT.fullmatch(str(doc.get("synapse", {}).get("uc_jdbc_connection") or "")):
        problems.append("synapse.uc_jdbc_connection must name the Unity Catalog JDBC connection used by the "
                        "serverless fallback")
    if doc.get("upstream", {}).get("completion_source") != "manifest":
        problems.append('upstream.completion_source must be "manifest" (4A uses the upstream row-count CSV and '
                        "its _SUCCESS marker; the bridge_upstream.upstream_run table is not installed)")
    return problems


def load_inventory(path):
    with open(path, encoding="utf-8") as f:
        return validate_inventory(json.load(f))
