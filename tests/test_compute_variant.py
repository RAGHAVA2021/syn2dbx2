from pathlib import Path
import copy
import json

import pytest
import yaml

from bridge import contract

ROOT = Path(__file__).resolve().parents[1]


def test_4a_nightly_path_is_all_serverless():
    bundle = yaml.safe_load((ROOT / 'databricks.yml').read_text())
    job = bundle['resources']['jobs']['nightly_full_refresh']
    tasks = {t['task_key']: t for t in job['tasks']}
    assert 'job_clusters' not in job
    assert 'job_cluster_key' not in tasks['plan']
    assert 'job_cluster_key' not in tasks['process']['for_each_task']['task']
    assert 'job_cluster_key' not in tasks['serverless_jdbc_fallback']['for_each_task']['task']
    assert 'job_cluster_key' not in tasks['publish']['for_each_task']['task']
    assert 'job_cluster_key' not in tasks['finalize']
    assert tasks['plan']['environment_key'] == 'serverless_sql'
    assert tasks['process']['for_each_task']['task']['environment_key'] == 'serverless_sql'
    deps = job['environments'][0]['spec']['dependencies']
    assert any(x.startswith('mssql-python==') for x in deps)


def test_4a_cetas_command_path_has_serverless_dbapi_support():
    code = (ROOT / 'src' / 'bridge' / 'synapse.py').read_text()
    assert 'def _serverless_dbapi_connection' in code
    assert 'import mssql_python' in code
    assert 'SQL_COPT_SS_ACCESS_TOKEN' in code
    assert 'Server=tcp:' in code


def test_4a_inventory_declares_uc_jdbc_connection():
    from bridge import contract
    for name in ('settings.example.json', 'inventory.example.json'):
        cfg = json.loads((ROOT / 'config' / name).read_text())
        assert cfg['synapse']['uc_jdbc_connection']
    # settings.example.json holds placeholders; the example inventory must pass the deploy check.
    assert contract.variant_4a_problems(json.loads((ROOT / 'config' / 'inventory.example.json').read_text())) == []


def test_4a_fallback_uses_governed_uc_connection():
    notebook = (ROOT / 'src' / 'notebooks' / '20_serverless_jdbc_fallback.py').read_text()
    loader = (ROOT / 'src' / 'bridge' / 'loader.py').read_text()
    assert 'loader.uc_jdbc_export' in notebook
    fn = loader[loader.index('def _uc_jdbc_reader'):loader.index('def uc_jdbc_export')]
    assert '.option("databricks.connection", connection_name)' in fn
    assert '.option("url"' not in fn


def test_4a_requires_manifest_completion():
    doc = json.loads((ROOT / 'config' / 'inventory.example.json').read_text())
    for upstream in ({"completion_source": "synapse_table"}, {}):
        doc["upstream"] = upstream
        assert any("completion_source" in p for p in contract.variant_4a_problems(doc))


def test_upstream_control_schema_is_needed_only_for_the_completion_table():
    doc = json.loads((ROOT / 'config' / 'inventory.example.json').read_text())
    assert "upstream_control_schema" not in doc["synapse"]
    contract.validate_inventory(copy.deepcopy(doc))          # manifest mode: not required
    doc["upstream"] = {"completion_source": "synapse_table"}
    with pytest.raises(Exception, match="upstream_control_schema"):
        contract.validate_inventory(copy.deepcopy(doc))
