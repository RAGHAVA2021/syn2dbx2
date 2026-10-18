"""Regression tests for the v2.6.8 deploy-workflow and documentation fixes."""
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _workflow(name):
    return yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text())


def _steps(doc):
    return [s for job in doc["jobs"].values() for s in job.get("steps", [])]


def test_deploy_workflow_pins_setup_cli_to_a_commit():
    uses = [s["uses"] for s in _steps(_workflow("bundle-deploy.yml"))
            if s.get("uses", "").startswith("databricks/setup-cli@")]
    assert uses, "bundle-deploy.yml no longer installs the Databricks CLI"
    for u in uses:
        assert re.fullmatch(r"databricks/setup-cli@[0-9a-f]{40}", u), u


def test_ci_and_deploy_use_the_same_python():
    def versions(name):
        return {str(s["with"]["python-version"]) for s in _steps(_workflow(name))
                if s.get("uses", "").startswith("actions/setup-python@")}
    assert versions("ci-cd.yml") == versions("bundle-deploy.yml") == {"3.12"}


def test_runbook_release_validation_references_exist():
    runbook = (ROOT / "docs" / "RUNBOOK.md").read_text()
    validation = (ROOT / "docs" / "RELEASE_VALIDATION.md").read_text()
    headings = {h.strip() for h in re.findall(r"^#+ (.+)$", validation, flags=re.M)}
    refs = re.findall(r'"([^"]+)"\s+(?:checks\s+)?in\s+`(?:docs/)?RELEASE_VALIDATION\.md`', runbook)
    assert refs, "RUNBOOK no longer references RELEASE_VALIDATION.md sections"
    for ref in refs:
        assert any(h.startswith(ref) for h in headings), ref


def test_release_validation_has_no_stale_variant_wording():
    text = (ROOT / "docs" / "RELEASE_VALIDATION.md").read_text()
    assert "4B" not in text and "serverless/dedicated" not in text
