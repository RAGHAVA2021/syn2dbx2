"""v1.2: reuse checks for existing Synapse objects and CETAS error classification."""
import pytest

from bridge import planning, synapse


def settings(root="abfss://staging@contosostage.dfs.core.windows.net"):
    return {"staging": {"root": root, "file_format": "bridge_parquet", "external_data_source": "bridge_stage"}}


def test_parquet_format_and_matching_location_pass():
    assert synapse.staging_problems("PARQUET", "abfss://staging@contosostage.dfs.core.windows.net/", settings()) == []
    assert synapse.staging_problems("parquet", "ABFSS://STAGING@contosostage.dfs.core.windows.net", settings()) == []


def test_delimited_text_format_is_refused():
    problems = synapse.staging_problems("DELIMITEDTEXT", "abfss://staging@contosostage.dfs.core.windows.net", settings())
    assert len(problems) == 1 and "must be PARQUET" in problems[0] and "line breaks, pipes" in problems[0]


def test_data_source_pointing_elsewhere_is_refused():
    problems = synapse.staging_problems("PARQUET", "abfss://other@contosostage.dfs.core.windows.net", settings())
    assert len(problems) == 1 and "points at" in problems[0]


def test_missing_objects_are_reported_together():
    problems = synapse.staging_problems(None, None, settings())
    assert len(problems) == 2 and all("does not exist" in p for p in problems)


def test_standalone_dedicated_pool_uses_matching_tls_hostname():
    from bridge import contract
    settings = {"synapse": {"server": "legacydw.database.windows.net", "database": "pool"}}
    assert contract.SERVER.fullmatch(settings["synapse"]["server"])
    assert "hostNameInCertificate=*.database.windows.net" in synapse.jdbc_url(settings)
    settings["synapse"]["server"] = "workspace.sql.azuresynapse.net"
    assert "hostNameInCertificate=*.sql.azuresynapse.net" in synapse.jdbc_url(settings)


def test_check_staging_objects_raises_with_all_problems():
    class Conn:
        def query(self, sql, *params):
            return [("DELIMITEDTEXT",)] if "file_formats" in sql else [("abfss://x@y.dfs.core.windows.net",)]
    with pytest.raises(RuntimeError, match="must be PARQUET.*points at"):
        synapse.check_staging_objects(Conn(), settings())


class JavaSQLException:
    def __init__(self, code):
        self.code = code

    def getErrorCode(self):
        return self.code


class Py4JLike(Exception):
    def __init__(self, code):
        super().__init__(f"SQLServerException code {code}")
        self.java_exception = JavaSQLException(code)


def test_error_code_read_from_py4j_exception_and_its_causes():
    assert synapse.sql_error_code(Py4JLike(8115)) == 8115
    try:
        try:
            raise Py4JLike(241)
        except Exception as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as outer:
        assert synapse.sql_error_code(outer) == 241
    assert synapse.sql_error_code(RuntimeError("no java")) is None
    assert synapse.sql_error_code(Py4JLike(0)) is None


def test_data_error_codes_are_deterministic_statuses():
    assert "FAILED_SOURCE_DATA" in planning.DETERMINISTIC_FAILURES
    assert "FAILED_SOURCE_DATA" not in planning.TRANSIENT_FAILURES


# ---- v1.3: classification and cause-chain walking ------------------------------

@pytest.mark.parametrize("code,state,kind", [
    (8115, None, "DATA"), (241, "22007", "DATA"),
    (40613, None, "CAPACITY"), (-2, None, "CAPACITY"), (None, None, "CAPACITY"),
    (12345, "08S01", "CAPACITY"), (12345, "HYT00", "CAPACITY"),
    (105019, "S0001", "CETAS"), (110802, None, "CETAS"),
])
def test_classify_export_error(code, state, kind):
    assert synapse.classify_export_error(code, state) == kind


class ChainedJava:
    def __init__(self, code, state=None, cause=None):
        self.code, self.state, self.cause = code, state, cause

    def getErrorCode(self):
        return self.code

    def getSQLState(self):
        return self.state

    def getCause(self):
        return self.cause


def test_error_info_found_deep_in_a_java_cause_chain():
    # SparkException(code 0) -> wrapper(code 0) -> SQLServerException(8115)
    java = ChainedJava(0, cause=ChainedJava(0, cause=ChainedJava(8115, "22003")))
    exc = RuntimeError("spark job failed")
    exc.java_exception = java
    assert synapse.sql_error_info(exc) == (8115, "22003")


def test_credentials_provider_modes():
    class Secrets:
        def get(self, scope, key):
            return f"{scope}/{key}"

    class DBU:
        secrets = Secrets()

        class credentials:
            @staticmethod
            def getServiceCredentialsProvider(name):
                class C:
                    def get_token(self, scope):
                        class T:
                            token = f"tok-{name}-{scope}"
                        return T()
                return C()

    sql = {"synapse": {"auth": {"mode": "sql_password", "secret_scope": "s", "user_key": "u", "password_key": "p"}}}
    assert synapse.credentials_provider(sql, DBU)() == {"user": "s/u", "password": "s/p"}
    sc = {"synapse": {"auth": {"mode": "service_credential", "credential_name": "c"}}}
    assert synapse.credentials_provider(sc, DBU)() == {"accessToken": f"tok-c-{synapse.SQL_SCOPE}"}
