import re
from pathlib import Path

import pytest
import yaml

from bridge import contract
import generate_inventory
import render_uc_setup

ROOT = Path(__file__).resolve().parents[1]


def test_generator_maps_common_types_and_hashes():
    rows = [("id", 1, "bigint", 8, 19, 0, 0), ("flag", 2, "bit", 1, 1, 0, 1), ("ts", 3, "datetime2", 8, 27, 7, 1)]
    m = generate_inventory.build_mapping("dbo", "Thing", rows, "cat", "gold", 123)
    assert m["mapping_id"] == "dbo__Thing" and m["target_table"] == "thing"
    assert m["source_schema_hash"] == contract.schema_hash(rows)
    assert [contract.column_contract(c)["target_type"] for c in m["columns"]] == ["bigint", "boolean", "timestamp_ntz"]


def test_generator_reports_every_unsupported_column():
    rows = [("a", 1, "xml", -1, 0, 0, 1), ("b", 2, "nvarchar", -1, 0, 0, 1), ("c", 3, "int", 4, 10, 0, 0)]
    with pytest.raises(contract.UnsupportedType) as e:
        generate_inventory.build_mapping("dbo", "T", rows, "cat", "gold")
    assert "a:" in str(e.value) and "b:" in str(e.value)


def test_render_creates_every_target_with_exact_types(inventory):
    sql = render_uc_setup.render(inventory, "sp-app-id", "analysts")
    assert "CREATE TABLE IF NOT EXISTS `analytics`.`gold`.`customer`" in sql
    assert "`created_at` TIMESTAMP_NTZ" in sql and "`region_id` INT" in sql and "`credit_limit` DECIMAL(19,4)" in sql
    assert "`updated_at` TIMESTAMP" in sql and "`_bridge_release_id` STRING" in sql
    assert "'delta.deletedFileRetentionDuration' = 'interval 7 days'" in sql
    assert "GRANT USE SCHEMA, SELECT ON SCHEMA `analytics`.`gold` TO `analysts`" in sql
    assert "GRANT SELECT, MODIFY ON TABLE `analytics`.`gold`.`customer` TO `sp-app-id`" in sql
    assert "READ METADATA" not in sql.upper().replace("_", " ")
    assert sql.count("CREATE TABLE IF NOT EXISTS") == len(inventory["mappings"]) + 3


def test_render_refuses_unsafe_principal(inventory):
    with pytest.raises(ValueError):
        render_uc_setup.render(inventory, "x` TO `everyone")


def test_control_log_schemas_match_render():
    from bridge import control
    for table, schema in (("table_log", control.TABLE_LOG), ("run_log", control.RUN_LOG), ("plan", control.PLAN)):
        ddl = render_uc_setup.CONTROL_TABLES[table]
        names = [part.strip().split()[0] for part in ddl.split(",")]
        assert names == control.field_names(schema), table


def test_bundle_references_existing_notebooks_and_parameters():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    paths = re.findall(r"notebook_path: (\S+)", (ROOT / "databricks.yml").read_text())
    assert paths and all((ROOT / p).exists() for p in paths)
    nightly = bundle["resources"]["jobs"]["nightly_full_refresh"]
    assert nightly["max_concurrent_runs"] == 1
    tasks = {t["task_key"]: t for t in nightly["tasks"]}
    assert tasks["finalize"]["run_if"] == "ALL_DONE"
    assert tasks["process"]["for_each_task"]["inputs"] == "{{tasks.plan.values.batch_ids}}"


def test_notebooks_and_modules_compile():
    for p in list((ROOT / "src").rglob("*.py")) + list((ROOT / "tools").glob("*.py")):
        compile(p.read_text(), str(p), "exec")


def test_widgets_used_by_notebooks_are_job_parameters_or_base_parameters():
    text = (ROOT / "databricks.yml").read_text()
    for nb in ("00_plan.py", "10_extract_validate_batch.py", "20_serverless_jdbc_fallback.py",
               "30_publish_batch.py", "90_finalize.py", "maintenance.py", "rollback.py",
               "acceptance_checks.py"):
        src = (ROOT / "src" / "notebooks" / nb).read_text()
        for name in re.findall(r'widget\("([a-z_]+)"', src):
            assert f"name: {name}" in text or f"{name}:" in text, f"{nb}: widget {name} not supplied"
