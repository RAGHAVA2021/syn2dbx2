"""v2.6.3 regressions: execution-scoped commit ownership (Repair runs), valid UC
grants, scope-qualified ABAC exemptions, finalize not retrying its own verdict."""
import json
from datetime import timedelta
from pathlib import Path

import pytest

from bridge import control, planning, process
from test_process import FakeOps, NOW

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def ctx(inventory):
    import test_process
    test_process.SETTINGS = inventory
    return process.Context(settings=inventory, release="synapse_nightly-2026092500", sequence=2026092500,
                           business_date="2026-09-25", job_run_id="42", cutoff_utc=NOW + timedelta(hours=10),
                           repair_count=0)


@pytest.fixture
def m(inventory):
    return inventory["mappings"][0]


def commit(ctx, version=5, rows=10, job_run_id="42", repair_count=0):
    md = planning.user_metadata(ctx.release, ctx.sequence, ctx.business_date, "m", job_run_id, "a", repair_count)
    return {"version": version, "operation": "WRITE", "userMetadata": md,
            "operationMetrics": {"numOutputRows": str(rows)}}


def older(version=4):
    return {"version": version, "operation": "WRITE", "userMetadata": None,
            "operationMetrics": {"numOutputRows": "9"}}


# ---- Fix 1: a Repair run keeps {{job.run_id}} ---------------------------------------

def test_repair_run_does_not_restore_the_original_attempts_commit(ctx, m):
    # Original attempt (repair 0) committed 10 rows and lost the ack. Finalize
    # released the fence, the upstream corrected to 12 rows, the operator repaired.
    ctx.repair_count, ctx.fence_released = 1, True
    ops = FakeOps(m, history=[commit(ctx, repair_count=0), older()], source_rows=12)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert ops.restored is None and "overwrite" not in ops.calls
    assert "not this execution" in ops.logs[-1]["detail"]


def test_released_fence_blocks_restore_even_for_the_same_repair_attempt(ctx, m):
    ctx.fence_released = True
    ops = FakeOps(m, history=[commit(ctx, repair_count=0), older()], source_rows=12)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert ops.restored is None
    assert "write fence was released" in ops.logs[-1]["detail"]


def test_legacy_commit_without_repair_count_is_not_owned(ctx, m):
    ops = FakeOps(m, history=[commit(ctx, repair_count=None), older()], source_rows=12)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY"
    assert ops.restored is None


def test_same_execution_lost_ack_bad_commit_is_still_restored(ctx, m):
    ops = FakeOps(m, history=[commit(ctx, repair_count=0), older()], source_rows=12)
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY_ROLLED_BACK"
    assert ops.restored == 4


def test_repair_run_still_confirms_a_good_commit(ctx, m):
    ctx.repair_count, ctx.fence_released = 1, True
    ops = FakeOps(m, history=[commit(ctx, repair_count=0), older()], source_rows=10)
    assert process.process_table(ctx, m, ops, now=NOW) == "ALREADY_CURRENT"


def test_new_commits_record_the_repair_attempt(ctx, m):
    ctx.repair_count = 2
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "PUBLISHED"
    md = json.loads(ops.hist[0]["userMetadata"])
    assert md["repair_count"] == 2 and md["job_run_id"] == "42"


def test_metadata_without_repair_count_is_unchanged():
    md = json.loads(planning.user_metadata("r", 2026092500, "2026-09-25", "m", "1", "a"))
    assert "repair_count" not in md


@pytest.mark.parametrize("value, expected", [("0", 0), (" 3 ", 3), (7, 7)])
def test_repair_count_parses(value, expected):
    assert control.parse_repair_count(value) == expected


@pytest.mark.parametrize("value", ["", "{{job.repair_count}}", "-1", "1.0", "x"])
def test_unresolved_or_bad_repair_count_fails_loudly(value):
    with pytest.raises(ValueError):
        control.parse_repair_count(value)


def test_process_batch_task_receives_the_repair_count():
    import yaml
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    tasks = {t["task_key"]: t for t in bundle["resources"]["jobs"]["nightly_full_refresh"]["tasks"]}
    params = tasks["process"]["for_each_task"]["task"]["notebook_task"]["base_parameters"]
    assert params["repair_count"] == "{{job.repair_count}}"
    notebook = (ROOT / "src/notebooks/10_extract_validate_batch.py").read_text()
    assert "parse_repair_count(widget(\"repair_count\"))" in notebook
    assert "fence_released=control.fence_released(" in notebook


# ---- Fix 2: every rendered GRANT uses a Unity Catalog privilege ----------------------

UC_PRIVILEGES = {"ALL PRIVILEGES", "APPLY TAG", "BROWSE", "EXECUTE", "MANAGE", "MODIFY", "READ FILES",
                 "READ VOLUME", "SELECT", "USE CATALOG", "USE SCHEMA", "WRITE FILES", "WRITE VOLUME",
                 "CREATE TABLE", "CREATE SCHEMA", "CREATE FUNCTION", "CREATE VOLUME", "REFRESH", "USE CONNECTION"}


def test_rendered_grants_use_only_unity_catalog_privileges(inventory):
    import re
    import sys
    sys.path.insert(0, str(ROOT / "tools"))
    import render_uc_setup
    sql = render_uc_setup.render(inventory, "sp-app-id", "analysts")
    grants = [line for line in sql.splitlines() if line.startswith("GRANT ")]
    assert grants
    for line in grants:
        privileges = re.match(r"GRANT (.+?) ON ", line).group(1)
        for p in privileges.split(","):
            assert p.strip().upper() in UC_PRIVILEGES, line


# ---- Fix 3: an ABAC exemption covers one policy on one securable ---------------------

from bridge import contract  # noqa: E402


def _describe():
    return json.dumps({"columns": [{"name": "id"}]})


def _row(name, catalog, schema=None, table=None, kind="COLUMN_MASK"):
    return {"Policy Name": name, "Policy Type": kind, "Catalog": catalog, "Schema": schema, "Table": table}


def _exempt(*entries):
    return contract.exempt_abac_policies({"governance": {"exempt_abac_policies": list(entries)}})


def test_exemption_does_not_cover_a_same_named_policy_on_another_securable():
    reviewed = _exempt({"catalog": "analytics", "name": "pii_mask"})
    rows = [_row("pii_mask", "analytics"),                    # the reviewed catalog policy
            _row("pii_mask", "analytics", "gold"),            # same name, schema level: unreviewed
            _row("pii_mask", "analytics", "gold", "customer"),  # same name, table level: unreviewed
            _row("pii_mask", "other_catalog")]                # same name, other catalog: unreviewed
    blocking, exempted = contract.governance_problems(rows, _describe(), reviewed)
    assert exempted == ["COLUMN_MASK:analytics/pii_mask"]
    assert blocking == ["COLUMN_MASK:analytics.gold/pii_mask", "COLUMN_MASK:analytics.gold.customer/pii_mask",
                        "COLUMN_MASK:other_catalog/pii_mask"]


def test_schema_and_table_scoped_exemptions_match_exactly_and_case_insensitively():
    reviewed = _exempt({"catalog": "Analytics", "schema": "Gold", "name": "PII_Mask"},
                       {"catalog": "analytics", "schema": "gold", "table": "customer", "name": "f"})
    rows = [_row("pii_mask", "analytics", "gold"), _row("f", "ANALYTICS", "GOLD", "CUSTOMER", "ROW_FILTER"),
            _row("pii_mask", "analytics")]
    blocking, exempted = contract.governance_problems(rows, _describe(), reviewed)
    assert exempted == ["COLUMN_MASK:analytics.gold/pii_mask", "ROW_FILTER:analytics.gold.customer/f"]
    assert blocking == ["COLUMN_MASK:analytics/pii_mask"]


def test_policy_row_without_its_catalog_is_never_exempt():
    reviewed = _exempt({"catalog": "analytics", "name": "pii_mask"})
    blocking, exempted = contract.governance_problems([{"Policy Name": "pii_mask", "Policy Type": "COLUMN_MASK"}],
                                                      _describe(), reviewed)
    assert exempted == [] and blocking == ["COLUMN_MASK:?/pii_mask"]


@pytest.mark.parametrize("entry", [
    "pii_mask",                                                   # legacy bare name: ambiguous
    {"name": "pii_mask"},                                         # no securable
    {"catalog": "analytics", "table": "t", "name": "pii_mask"},   # table without schema
    {"catalog": "analytics", "name": "pii_mask", "extra": 1},     # unknown key
    {"catalog": "bad catalog", "name": "pii_mask"},
])
def test_invalid_exemptions_fail_inventory_validation(inventory, entry):
    inventory["governance"] = {"exempt_abac_policies": [entry]}
    with pytest.raises(ValueError, match="exempt_abac_policies"):
        contract.validate_inventory(inventory)
    with pytest.raises(ValueError):
        contract.exempt_abac_policies(inventory)


# ---- Fix 4: finalize delivers its verdict once -----------------------------------------

def test_finalize_is_not_retried():
    import yaml
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    tasks = {t["task_key"]: t for t in bundle["resources"]["jobs"]["nightly_full_refresh"]["tasks"]}
    assert tasks["finalize"]["run_if"] == "ALL_DONE"
    assert tasks["finalize"].get("max_retries", 0) == 0
