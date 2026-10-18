"""Prepare the bundle for one environment before `databricks bundle deploy`.

  python tools/prepare_deploy.py --environment dev

The notebooks always read config/inventory.json (a fixed path, so a job
parameter cannot redirect it). Each environment keeps its own reviewed
inventory in Git as config/inventory.<environment>.json. This tool:

1. checks that the bundle target's workspace host is no longer a placeholder;
2. validates config/inventory.<environment>.json with the same rules the job uses,
   plus the 4A requirements (deferred fallback and a UC JDBC connection);
3. copies it to config/inventory.json, which the bundle then deploys.

It is run by .github/workflows/bundle-deploy.yml and can be run by hand.
Exit code is non-zero, with a clear message, on any problem.
"""
import argparse
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from bridge import contract  # noqa: E402

ENVIRONMENTS = ("dev", "prod")


def target_host(bundle_text, environment):
    """Workspace host of a bundle target, read without requiring PyYAML on the runner."""
    try:
        import yaml
    except ModuleNotFoundError:
        yaml = None
    if yaml is not None:
        targets = yaml.safe_load(bundle_text).get("targets", {})
        return (targets.get(environment) or {}).get("workspace", {}).get("host")
    lines = bundle_text.splitlines()
    inside = False
    for line in lines:
        if line.rstrip() == f"  {environment}:":
            inside = True
        elif inside and line.startswith("  ") and not line.startswith("   "):
            break
        elif inside and line.strip().startswith("host:"):
            return line.split("host:", 1)[1].strip()
    return None


def prepare(environment, root=ROOT):
    if environment not in ENVIRONMENTS:
        raise SystemExit(f"environment must be one of {ENVIRONMENTS}, got {environment!r}")
    host = target_host((root / "databricks.yml").read_text(), environment)
    if not host:
        raise SystemExit(f"databricks.yml has no workspace host for target {environment!r}")
    if "<" in host or ">" in host:
        raise SystemExit(f"databricks.yml target {environment!r} host is still a placeholder: {host}")
    source = root / "config" / f"inventory.{environment}.json"
    if not source.exists():
        raise SystemExit(f"{source.relative_to(root)} not found. Generate it with tools/generate_inventory.py, "
                         "review it, and commit it before deploying this environment.")
    doc = contract.load_inventory(source)
    problems = contract.variant_4a_problems(doc)
    if problems:
        raise SystemExit(f"{source.relative_to(root)}: " + "; ".join(problems))
    target = root / "config" / "inventory.json"
    shutil.copyfile(source, target)
    return {"environment": environment, "host": host, "inventory": str(source.relative_to(root)),
            "inventory_version": doc["inventory_version"], "mappings": len(doc["mappings"]),
            "enabled": sum(1 for m in doc["mappings"] if m.get("enabled", True)),
            "control": f"{doc['control']['catalog']}.{doc['control']['schema']}"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--environment", required=True, choices=ENVIRONMENTS)
    a = ap.parse_args()
    info = prepare(a.environment)
    print("Prepared bundle for deployment:")
    for k, v in info.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
