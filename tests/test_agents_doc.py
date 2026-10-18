"""AGENTS.md must stay truthful: every file, function and status it names must exist."""
import re
from pathlib import Path

from bridge import contract, planning, process, synapse, control  # noqa: F401

try:
    from bridge import loader
except ModuleNotFoundError as exc:
    if exc.name != "pyspark":
        raise
    loader = None

ROOT = Path(__file__).resolve().parents[1]
DOC = (ROOT / "AGENTS.md").read_text()
MODULES = {"contract": contract, "planning": planning, "process": process, "loader": loader,
           "synapse": synapse, "control": control}


def test_every_referenced_path_exists():
    paths = set(re.findall(r"`((?:src|tests|tools|sql|docs|config|infra)/[^`*<> ]+?)`", DOC))
    paths |= {"databricks.yml"}
    generated = {"config/inventory.json"}          # created by the operator, not shipped
    missing = [p for p in sorted(paths - generated) if not (ROOT / p).exists()]
    assert not missing, missing


def test_every_referenced_function_exists():
    refs = set(re.findall(r"`(contract|planning|process|loader|synapse|control)\.([A-Za-z_]+)`", DOC))
    settings_keys = {("synapse", "auth")}          # inventory keys that share a module name
    refs = {r for r in refs if r[1] != "py" and r not in settings_keys}
    missing = [f"{m}.{n}" for m, n in sorted(refs) if MODULES[m] is not None and not hasattr(MODULES[m], n)]
    assert refs and not missing, missing


def test_status_table_matches_code():
    documented = set(re.findall(r"^\| `([A-Z_]+)` \|", DOC, flags=re.M))
    code = planning.SUCCESS | planning.DETERMINISTIC_FAILURES | planning.TRANSIENT_FAILURES | planning.SKIPPED
    assert code <= documented, sorted(code - documented)
    assert documented - code <= planning.IN_FLIGHT | {"ROLLED_BACK"}, sorted(documented - code)


def test_referenced_test_names_exist():
    tests = "".join(p.read_text() for p in (ROOT / "tests").glob("test_*.py"))
    for name in re.findall(r"`(test_[a-z_]+)`", DOC):
        assert f"def {name}" in tests, name
