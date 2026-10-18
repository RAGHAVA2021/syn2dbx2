"""GitHub deployment support: per-environment inventory selection and bundle safety."""
import json
import shutil
from pathlib import Path

import pytest
import yaml

import prepare_deploy

ROOT = Path(__file__).resolve().parents[1]


def _repo(tmp_path, host="https://adb-1.1.azuredatabricks.net", inventory=True):
    (tmp_path / "config").mkdir()
    text = (ROOT / "databricks.yml").read_text().replace("https://<dev-workspace>.azuredatabricks.net", host)
    (tmp_path / "databricks.yml").write_text(text)
    if inventory:
        shutil.copyfile(ROOT / "config" / "inventory.example.json", tmp_path / "config" / "inventory.dev.json")
    return tmp_path


def test_prepare_copies_validated_environment_inventory(tmp_path):
    root = _repo(tmp_path)
    info = prepare_deploy.prepare("dev", root)
    assert info["mappings"] == 2
    assert (root / "config" / "inventory.json").read_text() == (root / "config" / "inventory.dev.json").read_text()


def test_prepare_rejects_placeholder_host(tmp_path):
    root = _repo(tmp_path, host="https://<dev-workspace>.azuredatabricks.net")
    with pytest.raises(SystemExit, match="placeholder"):
        prepare_deploy.prepare("dev", root)


def test_prepare_requires_environment_inventory(tmp_path):
    root = _repo(tmp_path, inventory=False)
    with pytest.raises(SystemExit, match="inventory.dev.json not found"):
        prepare_deploy.prepare("dev", root)
    assert not (root / "config" / "inventory.json").exists()


def test_prepare_rejects_invalid_inventory(tmp_path):
    root = _repo(tmp_path)
    doc = json.loads((root / "config" / "inventory.dev.json").read_text())
    doc["mappings"][0]["target_table"] = "bad name; drop"
    (root / "config" / "inventory.dev.json").write_text(json.dumps(doc))
    with pytest.raises(Exception):
        prepare_deploy.prepare("dev", root)
    assert not (root / "config" / "inventory.json").exists()


def test_target_host_fallback_parser_matches_yaml():
    text = (ROOT / "databricks.yml").read_text()
    for env in prepare_deploy.ENVIRONMENTS:
        assert prepare_deploy.target_host(text, env) == yaml.safe_load(text)["targets"][env]["workspace"]["host"]


def test_schedules_default_paused_and_come_from_variables():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    jobs = bundle["resources"]["jobs"]
    assert jobs["nightly_full_refresh"]["schedule"]["pause_status"] == "${var.nightly_pause_status}"
    assert jobs["maintenance"]["schedule"]["pause_status"] == "${var.maintenance_pause_status}"
    assert bundle["variables"]["nightly_pause_status"]["default"] == "PAUSED"
    assert bundle["variables"]["maintenance_pause_status"]["default"] == "PAUSED"


def test_environment_inventories_and_workflows_are_not_deployed():
    excludes = yaml.safe_load((ROOT / "databricks.yml").read_text())["sync"]["exclude"]
    assert "config/inventory.*.json" in excludes and ".github/**" in excludes


def test_workflows_deploy_both_targets_through_one_reusable_job():
    ci = yaml.safe_load((ROOT / ".github" / "workflows" / "ci-cd.yml").read_text())
    jobs = ci["jobs"]
    assert jobs["deploy-dev"]["with"]["environment"] == "dev"
    assert jobs["deploy-prod"]["with"]["environment"] == "prod"
    assert "deploy-dev" in jobs["deploy-prod"]["needs"]
    deploy = yaml.safe_load((ROOT / ".github" / "workflows" / "bundle-deploy.yml").read_text())
    steps = " ".join(s.get("run", "") for s in deploy["jobs"]["deploy"]["steps"])
    assert steps.index("prepare_deploy.py") < steps.index("bundle deploy")
