import pytest

from bridge import contract


def test_case_distinct_sources_are_rejected_before_case_insensitive_manifest_matching(inventory):
    a, b = inventory["mappings"][:2]
    b["source_schema"] = a["source_schema"]
    b["source_object"] = a["source_object"].swapcase()
    with pytest.raises(ValueError, match="differs only in case"):
        contract.validate_inventory(inventory)


def col(t, **kw):
    return {"name": "c", "sql_type": t, "max_length": kw.get("ml", 8), "precision": kw.get("p", 0),
            "scale": kw.get("s", 0)}


@pytest.mark.parametrize("sql_type,expr,parquet,target", [
    ("nvarchar", "[c]", "string", "string"),
    ("bit", "[c]", "boolean", "boolean"),
    ("tinyint", "CAST([c] AS int)", "int", "int"),
    ("smallint", "CAST([c] AS int)", "int", "int"),
    ("int", "[c]", "int", "int"),
    ("bigint", "[c]", "bigint", "bigint"),
    ("money", "CAST([c] AS decimal(19,4))", "decimal(19,4)", "decimal(19,4)"),
    ("smallmoney", "CAST([c] AS decimal(10,4))", "decimal(10,4)", "decimal(10,4)"),
    ("float", "CAST([c] AS float)", "double", "double"),
    ("real", "CAST([c] AS float)", "double", "double"),
    ("date", "[c]", "date", "date"),
    ("datetime", "CAST([c] AS datetime2(6))", "timestamp", "timestamp_ntz"),
    ("datetime2", "CAST([c] AS datetime2(6))", "timestamp", "timestamp_ntz"),
    ("smalldatetime", "CAST([c] AS datetime2(6))", "timestamp", "timestamp_ntz"),
    ("datetimeoffset", "CAST(SWITCHOFFSET([c], '+00:00') AS datetime2(6))", "timestamp", "timestamp"),
    ("time", "CAST([c] AS varchar(16))", "string", "string"),
    ("uniqueidentifier", "CAST([c] AS char(36))", "string", "string"),
])
def test_type_mapping(sql_type, expr, parquet, target):
    c = contract.column_contract(col(sql_type))
    assert (c["export_expr"], c["parquet_type"], c["target_type"]) == (expr, parquet, target)


def test_decimal_keeps_precision_and_scale():
    c = contract.column_contract(col("decimal", p=18, s=2))
    assert c["parquet_type"] == c["target_type"] == "decimal(18,2)"


@pytest.mark.parametrize("bad", [col("nvarchar", ml=-1), col("varbinary"), col("xml"), col("geography"),
                                 col("sql_variant"), col("decimal", p=40, s=2), col("decimal", p=10, s=11)])
def test_unsupported_types_are_refused(bad):
    with pytest.raises(contract.UnsupportedType):
        contract.column_contract(bad)


@pytest.mark.parametrize("name", ["a]b", "a b", "x;DROP", "1abc", "", "a'b", "a`b"])
def test_unsafe_column_names_are_refused(name):
    with pytest.raises(ValueError):
        contract.column_contract({"name": name, "sql_type": "int"})


def test_example_inventory_is_valid(inventory):
    contract.validate_inventory(inventory)


@pytest.mark.parametrize("path,value,message", [
    (("mappings", 0, "source_schema"), "dbo];--", "source_schema"),
    (("mappings", 0, "target_table"), "t`x", "target_table"),
    (("staging", "root"), "abfss://c@a.dfs.core.windows.net/sub", "staging.root"),
    (("synapse", "server"), "evil.example.com", "synapse.server"),
    (("execution", "cutoff_local_time"), "25:00", "cutoff_local_time"),
    (("mappings", 0, "mandatory"), "yes", "mandatory"),
    (("table_properties", "delta.x"), "a'b", "unsafe table property"),
])
def test_validation_rejects_unsafe_values(inventory, path, value, message):
    node = inventory
    for p in path[:-1]:
        node = node[p]
    node[path[-1]] = value
    with pytest.raises(ValueError, match=message):
        contract.validate_inventory(inventory)


def test_validation_rejects_case_insensitive_duplicates(inventory):
    m = inventory["mappings"][0]
    m["columns"].append(dict(m["columns"][0], name=m["columns"][0]["name"].upper()))
    with pytest.raises(ValueError, match="duplicate column"):
        contract.validate_inventory(inventory)


def test_validation_rejects_reserved_prefix_and_shared_target(inventory):
    inventory["mappings"][0]["columns"][0]["name"] = "_bridge_business_date"
    inventory["mappings"][1]["target_table"] = inventory["mappings"][0]["target_table"].upper()
    with pytest.raises(ValueError) as e:
        contract.validate_inventory(inventory)
    assert "reserved prefix" in str(e.value) and "more than one mapping" in str(e.value)


def test_validation_reports_all_problems_at_once(inventory):
    inventory["mappings"][0]["source_schema"] = "bad name"
    inventory["mappings"][1]["columns"][0]["sql_type"] = "xml"
    with pytest.raises(ValueError) as e:
        contract.validate_inventory(inventory)
    assert "source_schema" in str(e.value) and "xml" in str(e.value)


def test_cetas_sql(inventory):
    m = inventory["mappings"][1]
    sql = contract.cetas_sql(m, inventory, "x_t_1", "bridge-stage/2026-09-25/r/dbo__SalesOrder/a1/")
    assert sql.startswith("CREATE EXTERNAL TABLE [bridge_meta].[x_t_1] WITH (LOCATION = "
                          "'bridge-stage/2026-09-25/r/dbo__SalesOrder/a1/', DATA_SOURCE = [bridge_stage], "
                          "FILE_FORMAT = [bridge_parquet]) AS SELECT ")
    assert "CAST([amount] AS decimal(18,2)) AS [amount]" in sql
    assert sql.endswith("FROM [dbo].[SalesOrder]")


@pytest.mark.parametrize("loc", ["a/../b/", "a'b/", "a/b", "/abs/", "a b/"])
def test_cetas_refuses_unsafe_locations(inventory, loc):
    loc = loc if loc != "/abs/" else "..//"
    with pytest.raises(ValueError):
        contract.cetas_sql(inventory["mappings"][0], inventory, "x_1", loc)


def test_schema_hash_is_stable_and_sensitive():
    rows = [("id", 1, "INT", 4, 10, 0, 0), ("n", 2, "nvarchar", 100, 0, 0, 1)]
    assert contract.schema_hash(rows) == contract.schema_hash([tuple(r) for r in rows])
    assert contract.schema_hash(rows) != contract.schema_hash([rows[0], ("n", 2, "nvarchar", 200, 0, 0, 1)])


def test_target_schema_includes_metadata(inventory):
    s = contract.target_schema(inventory["mappings"][0])
    assert s["created_at"] == "timestamp_ntz" and s["region_id"] == "int"
    assert s["_bridge_business_date"] == "date" and s["_bridge_release_id"] == "string"


# ---- v1.2: datetime precision handling --------------------------------------

TRUNC = "DATEADD(nanosecond, -(DATEPART(nanosecond, {e}) % 1000), {e})"


def test_datetime2_scale7_truncates_instead_of_rounding():
    c = contract.column_contract(col("datetime2", p=27, s=7))
    assert c["export_expr"] == f"CAST({TRUNC.format(e='[c]')} AS datetime2(6))"


@pytest.mark.parametrize("sql_type,scale", [("datetime2", 6), ("datetime2", 3), ("datetime", 3), ("smalldatetime", 0)])
def test_types_that_cannot_overflow_use_a_plain_cast(sql_type, scale):
    assert contract.column_contract(col(sql_type, s=scale))["export_expr"] == "CAST([c] AS datetime2(6))"


def test_datetimeoffset_scale7_truncates_after_normalising_to_utc():
    c = contract.column_contract(col("datetimeoffset", p=34, s=7))
    utc = "SWITCHOFFSET([c], '+00:00')"
    assert c["export_expr"] == f"CAST({TRUNC.format(e=utc)} AS datetime2(6))"
    assert c["target_type"] == "timestamp"


def test_truncation_arithmetic_is_exact_at_the_extremes():
    # Model of the T-SQL expression on 100 ns ticks: never rounds up, never goes negative.
    for ns in (999_999_900, 123_456_700, 900, 0, 1_000):
        truncated = ns - (ns % 1000)
        assert truncated % 1000 == 0 and 0 <= truncated <= ns and ns - truncated < 1000


# ---- v1.3: settings, auth, partitioned fallback -------------------------------

def test_execution_defaults_apply_when_keys_are_absent(inventory):
    for k in contract.DEFAULT_EXECUTION:
        inventory["execution"].pop(k, None)
    contract.validate_inventory(inventory)
    assert contract.execution(inventory, "history_limit") == 200
    assert contract.execution(inventory, "jdbc_fallback_enabled") is True


@pytest.mark.parametrize("key,value", [("history_limit", 10), ("jdbc_num_partitions", 0),
                                       ("upstream_wait_minutes", 601), ("jdbc_fallback_enabled", "yes"),
                                       ("cutoff_timezone", "Mars/Olympus")])
def test_execution_settings_are_range_checked(inventory, key, value):
    inventory["execution"][key] = value
    with pytest.raises(ValueError, match=key):
        contract.validate_inventory(inventory)


def test_sql_password_auth_requires_secret_references(inventory):
    inventory["synapse"]["auth"] = {"mode": "sql_password", "secret_scope": "s", "user_key": "u"}
    with pytest.raises(ValueError, match="password_key"):
        contract.validate_inventory(inventory)
    inventory["synapse"]["auth"]["password_key"] = "p"
    contract.validate_inventory(inventory)


def test_partition_column_must_be_integer_or_date(inventory):
    m = inventory["mappings"][1]
    for bad in ("status", "no_such_column", "updated_at"):
        m["jdbc_partition_column"] = bad
        with pytest.raises(ValueError, match="jdbc_partition_column"):
            contract.validate_inventory(inventory)
    m["jdbc_partition_column"] = "order_id"
    contract.validate_inventory(inventory)
    assert contract.bounds_sql(m) == ("SELECT CAST(MIN([order_id]) AS varchar(40)) AS lo, "
                                      "CAST(MAX([order_id]) AS varchar(40)) AS hi FROM [dbo].[SalesOrder]")


def test_jdbc_select_uses_the_same_casting_projection_as_cetas(inventory):
    m = inventory["mappings"][1]
    q = contract.jdbc_select_sql(m)
    assert q == f"SELECT {contract.projection_sql(m)} FROM [dbo].[SalesOrder]"
    assert contract.projection_sql(m) in contract.cetas_sql(m, inventory, "x_1", "p/a/")


def test_enabled_flag_must_be_boolean(inventory):
    inventory["mappings"][0]["enabled"] = "no"
    with pytest.raises(ValueError, match="enabled"):
        contract.validate_inventory(inventory)


# ---- v1.4: one catalog query for every schema hash -----------------------------

def test_batched_hashes_equal_the_generator_hashes(inventory):
    import generate_inventory
    cust = [("customer_id", 1, "bigint", 8, 19, 0, 0), ("name", 2, "nvarchar", 400, 0, 0, 1)]
    order = [("order_id", 1, "bigint", 8, 19, 0, 0)]
    m1 = generate_inventory.build_mapping("dbo", "Customer", cust, "c", "g")
    m2 = generate_inventory.build_mapping("sales", "Order", order, "c", "g")
    missing = dict(m2, mapping_id="gone", source_object="Gone")
    # catalog rows arrive grouped by object, columns possibly out of order, names in any case
    rows = [("DBO", "customer", *cust[1]), ("dbo", "Customer", *cust[0]), ("sales", "Order", *order[0])]
    hashes = contract.hashes_from_rows(rows, [m1, m2, missing])
    assert hashes == {m1["mapping_id"]: m1["source_schema_hash"], m2["mapping_id"]: m2["source_schema_hash"],
                      "gone": None}


def test_all_columns_sql_has_one_parameter_per_schema():
    assert contract.all_columns_sql(3).count("?") == 3
