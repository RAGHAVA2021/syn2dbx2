"""Synapse dedicated SQL pool access from Databricks.

Command/control SQL (schema checks, counts and CETAS) supports two execution
contexts behind the same interface:
  * serverless Spark Connect: Microsoft ``mssql-python`` DB-API driver;
  * classic/dedicated Spark: the SQL Server JDBC driver through the JVM gateway.

The bulk JDBC fallback is separate: variant 4A uses a Unity Catalog JDBC
connection on serverless; variant 4B uses Spark JDBC on a dedicated cluster.

Authentication modes (synapse.auth.mode):
  * service_credential: Unity Catalog service credential (access connector managed
    identity). Entra token, no secret to rotate. Preferred.
  * client_secret: service principal; tenant/client id/secret in a secret scope.
  * sql_password: SQL user and password in a secret scope (matches an existing
    user/password JDBC setup; prefer an Entra mode when possible).
"""
from contextlib import contextmanager
import re

from . import contract

SQL_SCOPE = "https://database.windows.net/.default"


def credentials_provider(settings, dbutils):
    """Return a function giving fresh JDBC connection properties on every call.

    Tokens are fetched per connection, so a long night never uses an expired token.
    """
    auth = settings["synapse"]["auth"]
    if auth["mode"] == "sql_password":
        s = lambda k: dbutils.secrets.get(auth["secret_scope"], auth[k])
        return lambda: {"user": s("user_key"), "password": s("password_key")}
    if auth["mode"] == "service_credential":
        credential = dbutils.credentials.getServiceCredentialsProvider(auth["credential_name"])
    else:
        from azure.identity import ClientSecretCredential
        s = lambda k: dbutils.secrets.get(auth["secret_scope"], auth[k])
        credential = ClientSecretCredential(s("tenant_id_key"), s("client_id_key"), s("client_secret_key"))
    return lambda: {"accessToken": credential.get_token(SQL_SCOPE).token}


def jdbc_url(settings):
    syn = settings["synapse"]
    host = contract.SERVER.fullmatch(syn["server"])
    if host is None:
        raise ValueError("synapse.server must be a validated Azure Synapse dedicated-pool endpoint")
    certificate = ("*.database.windows.net" if syn["server"].endswith(".database.windows.net")
                   else "*.sql.azuresynapse.net")
    return (f"jdbc:sqlserver://{syn['server']}:1433;database={syn['database']};encrypt=true;"
            f"trustServerCertificate=false;hostNameInCertificate={certificate};loginTimeout=30;"
            f"applicationName=synapse-full-refresh")


class Connection:
    def __init__(self, jconn):
        self.j = jconn

    def query(self, sql, *params, timeout=600):
        """Run a query; every value comes back as str or None (callers convert)."""
        stmt = self.j.prepareStatement(sql)
        try:
            stmt.setQueryTimeout(int(timeout))
            for i, p in enumerate(params, start=1):
                stmt.setString(i, str(p))
            rs = stmt.executeQuery()
            n = rs.getMetaData().getColumnCount()
            rows = []
            while rs.next():
                rows.append(tuple(rs.getString(i) for i in range(1, n + 1)))
            return rows
        finally:
            stmt.close()

    def execute(self, sql, timeout):
        stmt = self.j.createStatement()
        try:
            stmt.setQueryTimeout(int(timeout))
            stmt.execute(sql)
        finally:
            stmt.close()


class DbapiConnection:
    """Small adapter giving mssql-python the same interface as the JVM connection."""
    def __init__(self, conn):
        self.j = conn

    def query(self, sql, *params, timeout=600):
        self.j.timeout = int(timeout)
        cur = self.j.cursor()
        try:
            cur.execute(sql, *params)
            return [tuple(None if v is None else str(v) for v in row) for row in cur.fetchall()]
        finally:
            cur.close()

    def execute(self, sql, timeout):
        self.j.timeout = int(timeout)
        cur = self.j.cursor()
        try:
            cur.execute(sql)
            # Drain additional result sets, if any, before closing the cursor.
            while getattr(cur, "nextset", lambda: False)():
                pass
        finally:
            cur.close()


def odbc_value(value):
    """Brace-quote an ODBC connection-string value so ';', '=', '{' or '}' cannot break it."""
    return "{" + str(value).replace("}", "}}") + "}"


def _serverless_dbapi_connection(settings, get_props):
    """Open a command-capable SQL connection without JVM access.

    ``mssql-python`` uses Direct Database Connectivity and supports Azure Synapse
    dedicated pools. Access-token auth keeps the existing service-credential /
    client-secret model; SQL auth remains available for legacy deployments.
    """
    import struct
    try:
        import mssql_python
    except ImportError as exc:   # the job environment did not install the driver
        raise RuntimeError(
            "mssql-python is not installed on this serverless task. The nightly tasks need "
            "mssql-python==1.15.0 from the job environment (databricks.yml environments / "
            "environment_key); run the acceptance job to prove the environment is applied.") from exc

    syn = settings["synapse"]
    props = get_props()
    server = syn["server"]
    database = syn["database"]
    base = (f"Server=tcp:{server},1433;Database={database};Encrypt=yes;"
            "TrustServerCertificate=no;Application Name=synapse-full-refresh;")
    kwargs = {"timeout": 30, "autocommit": True}
    if "accessToken" in props:
        token_bytes = str(props["accessToken"]).encode("utf-16le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        kwargs["attrs_before"] = {1256: token_struct}  # SQL_COPT_SS_ACCESS_TOKEN
        conn = mssql_python.connect(base, **kwargs)
    else:
        # Values originate only from Databricks Secrets and never from inventory identifiers.
        conn = mssql_python.connect(base + "UID=" + odbc_value(props["user"]) + ";PWD="
                                    + odbc_value(props["password"]) + ";", **kwargs)
    return conn


def _jvm_gateway(spark):
    try:
        return spark.sparkContext._gateway.jvm
    except Exception:
        # Serverless uses Spark Connect and deliberately exposes no JVM gateway.
        return None


@contextmanager
def connect(spark, settings, get_props, url=None):
    jvm = _jvm_gateway(spark)
    if jvm is None:
        conn = _serverless_dbapi_connection(settings, get_props)
        try:
            yield DbapiConnection(conn)
        finally:
            conn.close()
        return

    props = jvm.java.util.Properties()
    for k, v in get_props().items():
        props.setProperty(k, v)
    jconn = jvm.java.sql.DriverManager.getConnection(url or jdbc_url(settings), props)
    jconn.setAutoCommit(True)
    try:
        yield Connection(jconn)
    finally:
        jconn.close()


# ------------------------------------------------------------------ helpers

def upstream_run(conn, settings, business_date=None):
    """Return the highest-sequence COMPLETE run for the requested date."""
    schema = contract.q_sql(settings["synapse"]["upstream_control_schema"])
    cols = "upstream_run_id, CONVERT(char(10), business_date, 23), upstream_sequence"
    where = f"FROM {schema}.[upstream_run] WHERE source_set_id=? AND status='COMPLETE'"
    if business_date:
        rows = conn.query(f"SELECT TOP 2 {cols} {where} AND business_date=CAST(? AS date) "
                          "ORDER BY upstream_sequence DESC",
                          settings["source_set_id"], business_date)
        if len(rows) > 1 and int(rows[0][2]) == int(rows[1][2]):
            raise RuntimeError(f"Conflicting COMPLETE upstream runs at sequence {rows[0][2]} "
                               f"for {business_date}")
        if not rows:
            return None
    else:
        rows = conn.query(f"SELECT TOP 1 {cols} {where} ORDER BY upstream_sequence DESC", settings["source_set_id"])
        if not rows:
            return None
    run_id, date, seq = rows[0]
    return {"upstream_run_id": run_id, "business_date": date, "sequence": int(seq)}


def schema_hash(conn, m):
    rows = conn.query(contract.SYS_COLUMNS_SQL, f"[{m['source_schema']}].[{m['source_object']}]")
    if not rows:
        raise contract.ContractError("FAILED_SCHEMA_DRIFT",
                                     f"source object {m['source_schema']}.{m['source_object']} not found")
    return contract.schema_hash(rows)


def schema_hashes(conn, mappings, timeout=600):
    """Schema hash for every mapping with ONE catalog query (instead of one per table)."""
    schemas = sorted({m["source_schema"] for m in mappings})
    rows = conn.query(contract.all_columns_sql(len(schemas)), *schemas, timeout=timeout)
    return contract.hashes_from_rows(rows, mappings)


def count_rows(conn, m, timeout):
    return int(conn.query(contract.count_sql(m), timeout=timeout)[0][0])


def export(conn, m, settings, external_table, location, timeout):
    """CETAS the mapping to location, always dropping the external table definition afterwards."""
    try:
        conn.execute(contract.cetas_sql(m, settings, external_table, location), timeout)
    finally:
        try:
            conn.execute(contract.drop_external_sql(settings, external_table), 300)
        except Exception:  # best effort; maintenance drops leftovers
            pass


# ------------------------------------------------ staging objects (reuse check)

def staging_problems(file_format_type, data_source_location, settings):
    """Pure check of the existing Synapse objects the export reuses.

    * The file format must be PARQUET. A DELIMITEDTEXT format would let line
      breaks, pipes and quotes inside text corrupt rows and columns.
    * The data source must point at the container Databricks reads
      (staging.root); otherwise CETAS succeeds but writes files elsewhere.
    """
    st = settings["staging"]
    problems = []
    if file_format_type is None:
        problems.append(f"external file format {st['file_format']!r} does not exist")
    elif str(file_format_type).upper() != "PARQUET":
        problems.append(f"external file format {st['file_format']!r} is {file_format_type}, must be PARQUET "
                        "(delimited text breaks on line breaks, pipes and quotes in the data)")
    if data_source_location is None:
        problems.append(f"external data source {st['external_data_source']!r} does not exist")
    elif str(data_source_location).rstrip("/").lower() != st["root"].rstrip("/").lower():
        problems.append(f"external data source {st['external_data_source']!r} points at {data_source_location}, "
                        f"but staging.root is {st['root']}")
    return problems


def check_staging_objects(conn, settings):
    st = settings["staging"]
    fmt = conn.query("SELECT format_type FROM sys.external_file_formats WHERE name=?", st["file_format"])
    ds = conn.query("SELECT location FROM sys.external_data_sources WHERE name=?", st["external_data_source"])
    problems = staging_problems(fmt[0][0] if fmt else None, ds[0][0] if ds else None, settings)
    if problems:
        raise RuntimeError("Synapse staging objects are not usable: " + "; ".join(problems))


# ------------------------------------------------- export error classification

# Caused by the source data itself: the same SELECT fails the same way on every
# retry and by any export route, so no retry and no fallback.
DATA_ERROR_CODES = {
    220: "arithmetic overflow",
    232: "arithmetic overflow",
    241: "date/time conversion from string failed",
    242: "out-of-range date/time from string",
    245: "conversion to number failed",
    248: "conversion overflowed int",
    517: "date arithmetic overflow",
    8114: "data type conversion failed",
    8115: "arithmetic overflow converting value",
    8134: "divide by zero in the source view",
}

# Pool capacity or connectivity: a second export route competes for the same pool
# or cannot connect either, so no fallback; the normal single task retry applies.
CAPACITY_ERROR_CODES = {
    -2: "timeout", 1105: "filegroup full", 9002: "log full", 40197: "service error, retry",
    40501: "service busy", 40613: "database unavailable", 10928: "resource limit",
    10929: "resource limit", 49918: "not enough resources", 49919: "too many requests",
    49920: "too many operations",
}
CAPACITY_SQLSTATE_PREFIXES = ("08", "HYT")   # connection failures, timeouts
# ODBC/ISO SQLSTATE class 22 is "data exception" (numeric out of range, invalid
# datetime, division by zero, invalid cast value). SQL Server raises it for the
# statement's data, so the same SELECT fails by every route: never a fallback.
DATA_SQLSTATE_PREFIXES = ("22",)
DRIVER_SQLSTATE_PREFIXES = ("HY", "IM", "01")  # raised by the ODBC driver / manager, not the server
SCHEMA_ERROR_CODES = {207, 208}  # invalid source column or object after the plan
ACCESS_ERROR_CODES = {229, 230}  # source/DDL permission denied; retrying JDBC cannot grant access
# Other SQL Server "permission denied" codes that are LIKELY on a CETAS grant
# regression, but not yet observed from this pool. They are not built in
# (AGENTS.md section 8 requires evidence from a real failure). Confirm a code
# in the trial, then list it in execution.additional_access_error_codes; it
# then ends as FAILED_SOURCE_ACCESS instead of burning a JDBC fallback slot.
ACCESS_ERROR_CANDIDATES = {262: "permission denied in database (e.g. CREATE TABLE)",
                           297: "user does not have permission to perform this action",
                           300: "permission was denied on object",
                           15247: "user does not have permission to perform this action"}


# SQL Server message templates (English, as the dedicated pool returns them) for the
# error numbers this module classifies. mssql-python raises exceptions that carry
# neither the SQLSTATE nor the native error number, and a Spark-JDBC failure seen
# through Spark Connect arrives only as text, so the number is recovered from the
# message. Only text that demonstrably comes from SQL Server is matched.
_SQLSERVER_TEXT_MARKERS = ("[SQL Server]", "SQLServerException", "com.microsoft.sqlserver")
_MESSAGE_CODES = (
    (208, r"Invalid object name"),
    (207, r"Invalid column name"),
    (229, r"permission was denied on the object"),
    (230, r"permission was denied on the column"),
    (300, r"permission was denied on object"),
    (262, r"permission denied in database"),
    (297, r"The user does not have permission to perform this action"),
    (15247, r"(?<!The )\bUser does not have permission to perform this action"),
    (8115, r"Arithmetic overflow error converting"),
    (220, r"Arithmetic overflow error for data type"),
    (232, r"Arithmetic overflow error for type"),
    (8134, r"Divide by zero error encountered"),
    (241, r"Conversion failed when converting date and/or time from character string"),
    (242, r"data type resulted in an out-of-range value"),
    (245, r"Conversion failed when converting the"),
    (248, r"overflowed an int column"),
    (517, r"column caused an overflow"),
    (8114, r"Error converting data type"),
    (40501, r"The service is currently busy"),
    (40613, r"is not currently available"),
    (40197, r"The service has encountered an error processing your request"),
    (10929, r"Resource ID\s*:\s*\d+\. The .*minimum guarantee"),
    (10928, r"Resource ID\s*:\s*\d+\. The .*limit .*has been reached"),
    (1105, r"Could not allocate space for object"),
    (9002, r"The transaction log for database .* is full"),
    (49918, r"Not enough resources to process request"),
    (49919, r"Too many create or update operations in progress"),
    (49920, r"Too many operations in progress"),
)
_MESSAGE_PATTERNS = tuple((code, re.compile(rx)) for code, rx in _MESSAGE_CODES)
_UNKNOWN_SQLSTATE = re.compile(r"SQLSTATE code:\s*([0-9A-Z]{5})")
_MSSQL_STATES = None


def code_from_message(text):
    """Native SQL Server error number from a message text, or None (pure)."""
    text = str(text or "")
    if not any(marker in text for marker in _SQLSERVER_TEXT_MARKERS):
        return None
    for code, pattern in _MESSAGE_PATTERNS:
        if pattern.search(text):
            return code
    return None


def _mssql_python_states():
    """(exception class name, driver_error) -> SQLSTATE, from mssql-python's own table.

    mssql-python builds every exception from sqlstate_to_exception(sqlstate, msg)
    and keeps only the class and driver_error text, so reversing that table
    recovers the SQLSTATE exactly. Pairs shared by several states map to None.
    """
    global _MSSQL_STATES
    if _MSSQL_STATES is None:
        table = {}
        try:
            import inspect
            from mssql_python import exceptions as mexc
            for state in set(re.findall(r'"([0-9A-Z]{5})"\s*:', inspect.getsource(mexc.sqlstate_to_exception))):
                e = mexc.sqlstate_to_exception(state, "")
                if e is not None:
                    key = (type(e).__name__, str(e.driver_error))
                    table[key] = None if key in table and table[key] != state else state
        except Exception:  # noqa: BLE001 - driver absent (tests, classic compute)
            table = {}
        _MSSQL_STATES = table
    return _MSSQL_STATES


def _mssql_python_info(exc):
    """(code, SQLSTATE) for an mssql-python exception, else None when exc is not one."""
    driver_error, ddbc_error = getattr(exc, "driver_error", None), getattr(exc, "ddbc_error", None)
    if not isinstance(driver_error, str) or not isinstance(ddbc_error, str):
        return None
    match = _UNKNOWN_SQLSTATE.search(driver_error)
    state = match.group(1) if match else _mssql_python_states().get((type(exc).__name__, driver_error))
    return code_from_message(ddbc_error), state


def sql_error_info(exc):
    """(error code, SQLSTATE) from JVM JDBC, mssql-python or text-only exceptions.

    The serverless command path uses ``mssql-python``, whose exceptions keep only
    the exception class and message; classic paths surface Java SQLServerException
    objects; Spark-JDBC errors over Spark Connect are text. Each shape is decoded
    so that the same SQL failure is classified identically on every route.
    """
    seen = set()
    first = exc
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        java = getattr(exc, "java_exception", None)
        depth = 0
        while java is not None and depth < 20:
            depth += 1
            try:
                code = int(java.getErrorCode())
                if code:
                    try:
                        state = java.getSQLState()
                    except Exception:
                        state = None
                    return code, (str(state) if state else None)
            except Exception:
                pass
            try:
                java = java.getCause()
            except Exception:
                java = None
        info = _mssql_python_info(exc)
        if info is not None and (info[0] or info[1]):
            return info
        # Drivers that expose the details as attributes (pyodbc-style wrappers).
        state = getattr(exc, "sqlstate", None)
        code = getattr(exc, "native_error", None)
        if code is not None:
            try:
                code = int(code)
            except (TypeError, ValueError):
                code = None
        if state or code:
            return code, (str(state) if state else None)
        # Common DB-API shape: ('42000', '[Microsoft]... (105019)')
        args = getattr(exc, "args", ()) or ()
        if args:
            first_arg = args[0]
            if isinstance(first_arg, str) and re.fullmatch(r"[0-9A-Z]{5}", first_arg.upper()):
                state = first_arg.upper()
            text = " ".join(str(x) for x in args)
            native = re.findall(r"\((-?\d+)\)(?:\s|$)", text)
            if native:
                try:
                    code = int(native[-1])
                except ValueError:
                    code = None
            if state or code:
                return code, state
        exc = exc.__cause__ or exc.__context__
    # Last resort: SQL Server text anywhere in the chain (Spark Connect JDBC errors).
    exc, seen = first, set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        code = code_from_message(f"{exc} {getattr(exc, 'ddbc_error', '')}")
        if code:
            return code, None
        exc = exc.__cause__ or exc.__context__
    return None, None


def sql_error_code(exc):
    return sql_error_info(exc)[0]


def classify_export_error(code, sqlstate, extra_access_codes=()):
    """Classify known deterministic failures before considering a JDBC fallback.

    extra_access_codes: confirmed access-denied codes from
    execution.additional_access_error_codes (validated not to overlap the
    schema, data or capacity sets).
    """
    if code in SCHEMA_ERROR_CODES:
        return "SCHEMA"
    if code in ACCESS_ERROR_CODES or (code is not None and code in set(extra_access_codes or ())):
        return "ACCESS"
    if code in DATA_ERROR_CODES:
        return "DATA"
    if code in CAPACITY_ERROR_CODES or (sqlstate and str(sqlstate).upper().startswith(CAPACITY_SQLSTATE_PREFIXES)):
        return "CAPACITY"
    if code is None and sqlstate and str(sqlstate).upper().startswith(DATA_SQLSTATE_PREFIXES):
        # mssql-python keeps the SQLSTATE but not the number; a message outside
        # _MESSAGE_CODES (e.g. 8169, uniqueidentifier conversion) is still data.
        return "DATA"
    if code is None:
        # No error number. With a server-reported SQLSTATE (mssql-python keeps the
        # state but not the number) it is a SQL Server failure of the statement
        # itself; driver-side (HY/IM) or no state at all is client/network: retry.
        if sqlstate and not str(sqlstate).upper().startswith(DRIVER_SQLSTATE_PREFIXES):
            return "CETAS"
        return "CAPACITY"
    return "CETAS"


# ------------------------------------------------ plan-time transient errors

TRANSIENT_JAVA_CLASS_PREFIXES = ("java.net.", "javax.net.ssl.SSLException")
TRANSIENT_JAVA_CLASSES = {"java.io.InterruptedIOException", "java.util.concurrent.TimeoutException",
                          "java.io.EOFException"}


def is_transient_platform_error(exc):
    """Errors the plan may retry: pool capacity/connectivity or a network-level failure.

    Narrow on purpose. Only a real SQL Server capacity code, a connection or
    timeout SQLSTATE, or a Java networking exception in the cause chain
    qualifies. Unlike classify_export_error, an error with no SQL code at all
    is NOT transient here: a malformed manifest, wrong staging objects or a
    bug must fail the plan at once. Type-based, not message-based (Runtime 18
    changed exception types for SQLSTATE).
    """
    from .manifest import MISSING_PATH_JAVA_CLASSES, java_class_name, java_exception_chain
    code, state = sql_error_info(exc)
    if code in CAPACITY_ERROR_CODES:
        return True
    if state and str(state).upper().startswith(CAPACITY_SQLSTATE_PREFIXES):
        return True
    for java in java_exception_chain(exc):
        name = java_class_name(java)
        if name in MISSING_PATH_JAVA_CLASSES:
            return False
        if name in TRANSIENT_JAVA_CLASSES or name.startswith(TRANSIENT_JAVA_CLASS_PREFIXES):
            return True
        # mssql-jdbc reports a dropped connection with error code 0 and SQLSTATE
        # 08S01, which sql_error_info (first non-zero code) does not return.
        try:
            java_state = java.getSQLState()
        except Exception:  # noqa: BLE001 - not a SQLException
            java_state = None
        if java_state and str(java_state).upper().startswith(CAPACITY_SQLSTATE_PREFIXES):
            return True
    return False
