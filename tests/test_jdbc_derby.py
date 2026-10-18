"""Real JDBC round trips against Derby (bundled with PySpark), no Synapse needed.

Covers the pieces the mocks cannot: the JVM DriverManager connection used for
commands, error-code extraction from a genuine Java exception chain, and the
fallback's Parquet staging (loader.stage_jdbc_query, which loader.uc_jdbc_export
uses; unpartitioned and partitioned) feeding the normal Parquet validation. Only
the reader differs: a plain JDBC URL here, the Unity Catalog connection in
production. SQL dialect differences mean the T-SQL projection itself is not run
here; the fallback wraps it unchanged (see test_contract).
"""
import datetime as dt
import decimal
import os

import pytest

pytest.importorskip("pyspark")

from bridge import loader, synapse  # noqa: E402

URL = "jdbc:derby:memory:bridge_it;create=true"
NO_PROPS = lambda: {}   # noqa: E731
VALUES = [(1, "line1\nline2", "2026-09-25 00:30:00", "2026-09-25", "10.50"),
          (2, "a|b|c", "2026-09-25 23:59:59.123456", "2026-09-24", "-3.25"),
          (3, 'say "hi"', "9999-12-31 23:59:59.999999", "2026-01-01", "0.00"),
          (4, None, None, None, None),
          (5, "emoji 😀", "2026-02-28 12:00:00", "2026-02-28", "99999999.99")]
MAPPING = {"columns": [
    {"name": "ID", "sql_type": "int"}, {"name": "TXT", "sql_type": "nvarchar", "max_length": 400},
    {"name": "TS", "sql_type": "datetime2", "scale": 6}, {"name": "D", "sql_type": "date"},
    {"name": "AMT", "sql_type": "decimal", "precision": 10, "scale": 2}]}


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("bridge-jdbc-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh"))).getOrCreate())
    with synapse.connect(s, None, NO_PROPS, url=URL) as conn:
        conn.execute("CREATE TABLE SRC (ID INT, TXT VARCHAR(200), TS TIMESTAMP, D DATE, AMT DECIMAL(10,2))", 30)
        def lit(v, kind):
            if v is None:
                return "NULL"
            if kind == "int":
                return str(v)
            text = "'" + v.replace("'", "''") + "'"
            return {"txt": text, "ts": f"TIMESTAMP({text})", "d": f"DATE({text})",
                    "amt": f"CAST({text} AS DECIMAL(10,2))"}[kind]
        for r in VALUES:
            vals = ", ".join(lit(v, k) for v, k in zip(r, ("int", "txt", "ts", "d", "amt")))
            conn.execute(f"INSERT INTO SRC VALUES ({vals})", 30)
    yield s
    s.stop()


def test_command_connection_query_and_parameters(spark):
    with synapse.connect(spark, None, NO_PROPS, url=URL) as conn:
        rows = conn.query("SELECT ID, TXT FROM SRC WHERE ID = CAST(? AS INT)", 2)
        assert rows == [("2", "a|b|c")]
        assert conn.query("SELECT COUNT(*) FROM SRC")[0][0] == "5"
        lo, hi = conn.query("SELECT MIN(ID), MAX(ID) FROM SRC")[0]   # getString returns numbers as text
        assert (lo, hi) == ("1", "5")


def test_error_info_is_extracted_from_a_real_java_exception(spark):
    with synapse.connect(spark, None, NO_PROPS, url=URL) as conn:
        with pytest.raises(Exception) as e:
            conn.execute("SELECT * FROM NO_SUCH_TABLE", 30)
    code, state = synapse.sql_error_info(e.value)
    assert code is not None and state == "42X05"      # Derby: table does not exist


def _reader(spark):
    # The same reader options as loader._uc_jdbc_reader, with a URL instead of a UC connection.
    return spark.read.format("jdbc").option("url", URL).option("preferTimestampNTZ", "true")


def _export(spark, tmp_path, **kw):
    path = str(tmp_path / "jdbc")
    loader.stage_jdbc_query(_reader(spark), "SELECT ID, TXT, TS, D, AMT FROM SRC", path, fetch_size=2, **kw)
    return path


def _check(spark, path):
    raw = loader.read_export(spark, path, MAPPING, os.listdir)
    df, rows = loader.validate_and_convert(raw, MAPPING, "2026-09-25", "rel-1")
    got = {r["ID"]: r for r in df.collect()}
    assert rows == len(VALUES)
    assert got[1]["TXT"] == "line1\nline2" and got[2]["TXT"] == "a|b|c" and got[5]["TXT"] == "emoji 😀"
    assert got[1]["TS"] == dt.datetime(2026, 9, 25, 0, 30)                       # wall clock, no shift
    assert got[3]["TS"] == dt.datetime(9999, 12, 31, 23, 59, 59, 999999)         # end-of-time sentinel
    assert got[2]["D"] == dt.date(2026, 9, 24) and got[2]["AMT"] == decimal.Decimal("-3.25")
    assert got[4]["TXT"] is None and got[4]["TS"] is None
    return raw


def test_fallback_export_unpartitioned_round_trips_through_validation(spark, tmp_path):
    raw = _check(spark, _export(spark, tmp_path))
    ts_type = dict((f.name, f.dataType.simpleString()) for f in raw.schema.fields)["TS"]
    assert ts_type == "timestamp_ntz"          # preferTimestampNTZ: independent of the JVM time zone


def test_fallback_export_partitioned_reads_in_parallel_and_matches(spark, tmp_path):
    path = _export(spark, tmp_path, bounds=("1", "5"), partition_column="ID", num_partitions=3)
    data_files = [f for f in os.listdir(path) if f.endswith(".parquet")]
    assert len(data_files) >= 2                # really split into parallel reads
    _check(spark, path)


def test_fallback_never_overwrites_an_existing_folder(spark, tmp_path):
    path = _export(spark, tmp_path)
    with pytest.raises(Exception, match="(?i)already exists"):
        loader.stage_jdbc_query(_reader(spark), "SELECT ID FROM SRC", path, fetch_size=2)
