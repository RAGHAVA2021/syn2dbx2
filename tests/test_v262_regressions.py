"""v2.6.2 regressions: Runtime 18 review fixes (Spark-free; runs without PySpark)."""
import ast
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from bridge import contract, manifest, planning, process, synapse
from test_process import FakeOps, NOW, ctx, m  # noqa: F401 (fixture imports)

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------- fake py4j errors

class JavaThrowable:
    def __init__(self, class_name, cause=None, code=0, state=None):
        self.class_name, self.cause, self.code, self.state = class_name, cause, code, state

    def getClass(self):
        name = self.class_name

        class _Class:
            def getName(self):
                return name
        return _Class()

    def getCause(self):
        return self.cause

    def getErrorCode(self):
        return self.code

    def getSQLState(self):
        return self.state


class Py4JLike(Exception):
    """A py4j/PySpark error whose message deliberately contains no known text."""

    def __init__(self, java, message="An error occurred while calling o123.ls"):
        super().__init__(message)
        self.java_exception = java


class ConditionError(Exception):
    def __init__(self, condition, message="failed"):
        super().__init__(message)
        self.condition = condition

    def getCondition(self):
        return self.condition


# ------------------------------------------------------------- 1. acceptance mappings

def _acceptance_mapping_literals():
    tree = ast.parse((ROOT / "src" / "notebooks" / "acceptance_checks.py").read_text())
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.endswith("_mapping"):
                    found[target.id] = {k.value for k in node.value.keys if isinstance(k, ast.Constant)}
    return found


def test_acceptance_mappings_carry_every_key_the_loader_needs():
    """v2.6.1 shipped mappings without mapping_id: the first write raised KeyError."""
    mappings = _acceptance_mapping_literals()
    assert {"scratch_mapping", "partition_mapping"} <= set(mappings)
    for name, keys in mappings.items():
        missing = {"mapping_id", "target_catalog", "target_schema", "target_table", "columns"} - keys
        assert not missing, f"{name} lacks {sorted(missing)}"


# ----------------------------------------------------- 2. governance (pure decision)

def _describe(**extra):
    return json.dumps({"columns": [{"name": "id"}], **extra})


def test_governance_inherited_policy_blocks_until_reviewed():
    rows = [{"Policy Name": "catalog_pii_mask", "Policy Type": "COLUMN_MASK", "Catalog": "analytics",
             "Schema": None, "Table": None}]
    assert contract.governance_problems(rows, _describe()) == (["COLUMN_MASK:analytics/catalog_pii_mask"], [])
    exempt = contract.exempt_abac_policies(
        {"governance": {"exempt_abac_policies": [{"catalog": "analytics", "name": "catalog_pii_mask"}]}})
    assert contract.governance_problems(rows, _describe(), exempt) == \
        ([], ["COLUMN_MASK:analytics/catalog_pii_mask"])


def test_table_level_policies_can_never_be_exempted():
    blocking, _ = contract.governance_problems([], _describe(row_filter={"filter_function": {}},
                                                             column_masks=[{"column_name": "x"}]),
                                               {"row_filter", "column_masks"})
    assert blocking == ["table-level row_filter", "table-level column_masks"]


def test_governance_rejects_untrustworthy_metadata():
    with pytest.raises(ValueError):
        contract.governance_problems([], json.dumps({"no_columns": True}))
    with pytest.raises(ValueError):
        contract.governance_problems([], "not json")


def test_governance_settings_are_validated(inventory):
    inventory["governance"] = {"exempt_abac_policies": [{"catalog": "analytics", "name": "bad name; DROP"}]}
    with pytest.raises(ValueError, match="exempt_abac_policies"):
        contract.validate_inventory(inventory)
    inventory["governance"] = {"exempt_abac_policies": [{"catalog": "analytics", "name": "pii_mask"}]}
    contract.validate_inventory(inventory)
    assert contract.exempt_abac_policies(inventory) == {("analytics", None, None, "pii_mask")}
    assert contract.exempt_abac_policies({}) == set()


@pytest.mark.parametrize("version,major", [("18.x-scala2.13", 18), ("17.3.x-scala2.13", 17),
                                           ("16.4.x-scala2.12", 16), ("custom:x__15.4.x-cpu__y", 15),
                                           (None, None), ("", None), ("latest", None)])
def test_runtime_major(version, major):
    assert contract.runtime_major(version) == major


# ------------------------------------------------------------ 3. finalize failure ratio

def test_failure_ratio_counts_every_non_success():
    plan = [{"mapping_id": str(i), "mandatory": False} for i in range(100)]
    statuses = {str(i): "PUBLISHED" for i in range(97)}
    statuses.update({"97": "FAILED_TARGET_SCHEMA", "98": "SKIPPED_CUTOFF"})   # "99" never started
    assert planning.failure_ratio(plan, statuses) == pytest.approx(0.03)
    assert planning.failure_ratio([], {}) == 0.0


def test_failure_ratio_setting_is_validated(inventory):
    inventory["execution"]["max_failed_table_ratio"] = 1.5
    with pytest.raises(ValueError, match="max_failed_table_ratio"):
        contract.validate_inventory(inventory)
    inventory["execution"]["max_failed_table_ratio"] = 0
    contract.validate_inventory(inventory)


def test_finalize_enforces_the_failure_ratio():
    text = (ROOT / "src" / "notebooks" / "90_finalize.py").read_text()
    assert "planning.failure_ratio(" in text and 'summary["failure_ratio_alert"]' in text


# ---------------------------------------------------- 4. plan transient-error retries

def test_capacity_and_connection_errors_are_transient():
    assert synapse.is_transient_platform_error(Py4JLike(JavaThrowable("com.microsoft...SQLServerException",
                                                                      code=40613)))
    # A dropped connection: error code 0, SQLSTATE 08S01 (not visible to sql_error_info).
    assert synapse.is_transient_platform_error(Py4JLike(JavaThrowable("x.SQLServerException", state="08S01")))
    assert synapse.is_transient_platform_error(Py4JLike(JavaThrowable("x.SQLServerException", state="HYT00")))
    assert synapse.is_transient_platform_error(
        Py4JLike(JavaThrowable("java.lang.RuntimeException", cause=JavaThrowable("java.net.SocketTimeoutException"))))


def test_deterministic_errors_are_not_transient():
    """Unlike export classification, 'no SQL code at all' must not be retried by the plan."""
    assert not synapse.is_transient_platform_error(manifest.ManifestError("manifest mixes business dates"))
    assert not synapse.is_transient_platform_error(RuntimeError("Synapse staging objects are not usable"))
    assert not synapse.is_transient_platform_error(Py4JLike(JavaThrowable("x.SQLServerException", code=208)))
    assert not synapse.is_transient_platform_error(Py4JLike(JavaThrowable("java.io.FileNotFoundException")))


def test_call_with_transient_retry_retries_then_succeeds_or_gives_up():
    clock = {"now": NOW}
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += timedelta(seconds=seconds)

    attempts = iter([ConnectionError("blip"), ConnectionError("blip"), "ok"])

    def flaky():
        value = next(attempts)
        if isinstance(value, Exception):
            raise value
        return value
    is_transient = lambda e: isinstance(e, ConnectionError)
    assert planning.call_with_transient_retry(flaky, is_transient, NOW + timedelta(minutes=10),
                                              now=lambda: clock["now"], sleep=sleep) == "ok"
    assert sleeps == [60, 60]

    def always():
        raise ConnectionError("down")
    with pytest.raises(ConnectionError):
        planning.call_with_transient_retry(always, is_transient, NOW + timedelta(minutes=3),
                                           now=lambda: clock["now"], sleep=sleep)

    def deterministic():
        raise ValueError("malformed")
    sleeps.clear()
    with pytest.raises(ValueError):
        planning.call_with_transient_retry(deterministic, is_transient, NOW + timedelta(hours=1),
                                           now=lambda: clock["now"], sleep=sleep)
    assert sleeps == []


def test_plan_notebook_uses_the_bounded_transient_retry():
    text = (ROOT / "src" / "notebooks" / "00_plan.py").read_text()
    assert "synapse.is_transient_platform_error" in text and "planning.call_with_transient_retry" in text
    assert "time.monotonic" not in text


# ----------------------------------------- 5. missing-path detection without message text

def test_missing_path_is_recognized_by_type_or_condition():
    assert manifest.is_missing_path_error(FileNotFoundError("x"))
    assert manifest.is_missing_path_error(Py4JLike(JavaThrowable("java.io.FileNotFoundException")))
    assert manifest.is_missing_path_error(
        Py4JLike(JavaThrowable("org.apache.hadoop.X", cause=JavaThrowable("java.io.FileNotFoundException"))))
    assert manifest.is_missing_path_error(ConditionError("PATH_NOT_FOUND"))
    wrapped = RuntimeError("outer")
    wrapped.__cause__ = Py4JLike(JavaThrowable("java.io.FileNotFoundException"))
    assert manifest.is_missing_path_error(wrapped)


def test_other_storage_errors_are_not_missing():
    assert not manifest.is_missing_path_error(
        Py4JLike(JavaThrowable("org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException")))
    assert not manifest.is_missing_path_error(ConditionError("INSUFFICIENT_PERMISSIONS"))


def test_no_notebook_matches_missing_paths_by_hand():
    for nb in (ROOT / "src" / "notebooks").glob("*.py"):
        text = nb.read_text()
        assert '"FileNotFoundException" in' not in text and '"does not exist" in' not in text, nb.name


def test_ancient_datetime_is_recognized_by_condition(ctx, m):
    ops = FakeOps(m)

    def ancient(*args, **kwargs):
        raise ConditionError("INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME", "opaque text")
    ops.validate_and_convert = ancient
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_CONTRACT"


# ---------------------------------------------- 6. margin also bounds new exports

def test_margin_setting_is_part_of_the_export_window(ctx, m):
    ctx.cutoff_utc = NOW + timedelta(minutes=5)          # exactly the default margin
    ops = FakeOps(m)
    assert process.process_table(ctx, m, ops, now=NOW) == "SKIPPED_CUTOFF"
    assert "export" not in ops.calls


# ------------------------------------ 9. dynamic partition overwrite is a bad publish

def _commit(md, version=3, rows=10, dynamic=None):
    params = {"mode": "Overwrite"}
    if dynamic is not None:
        params["isDynamicPartitionOverwrite"] = dynamic
    return {"version": version, "operation": "WRITE", "userMetadata": md, "operationParameters": params,
            "operationMetrics": {"numOutputRows": str(rows)}}


def test_dynamic_partition_overwrite_flag_fails_verification():
    md = planning.user_metadata("rel-1", 2026092500, "2026-09-25", "dbo__Customer", "42", "a")
    assert planning.verify_after_write([_commit(md)], "rel-1", 2026092500, 10, "2026-09-25")[0] == "PUBLISHED"
    assert planning.verify_after_write([_commit(md, dynamic="false")], "rel-1", 2026092500, 10,
                                       "2026-09-25")[0] == "PUBLISHED"
    status, version, detail = planning.verify_after_write([_commit(md, dynamic="true")], "rel-1",
                                                          2026092500, 10, "2026-09-25")
    assert status == "FAILED_VERIFY_NEEDS_ROLLBACK" and version == 3 and "dynamic partition" in detail


def test_dynamic_partition_overwrite_is_never_a_rollback_candidate():
    md = planning.user_metadata("rel-1", 2026092500, "2026-09-25", "dbo__Customer", "42", "a")
    assert planning.rollback_candidates([_commit(md)], {3: 10}, "2026-09-25") == [3]
    assert planning.rollback_candidates([_commit(md, dynamic="true")], {3: 10}, "2026-09-25") == []


def test_own_run_dynamic_commit_is_restored(ctx, m):
    """The live write path treats the flag like a wrong count: restore this run's own commit."""
    ops = FakeOps(m)
    real_overwrite = ops.overwrite

    def flagged(mapping, converted, sequence, metadata):
        real_overwrite(mapping, converted, sequence, metadata)
        ops.hist[0]["operationParameters"] = {"isDynamicPartitionOverwrite": "true"}
    ops.overwrite = flagged
    ops.hist = [{"version": 0, "operation": "CREATE TABLE", "userMetadata": None, "operationMetrics": {}}]
    assert process.process_table(ctx, m, ops, now=NOW) == "FAILED_VERIFY_ROLLED_BACK"
    assert ops.restored == 0


# ----------------------------------------------------------- 8. housekeeping

def test_docs_no_longer_require_installing_azure_identity_on_runtime_18():
    for doc in ("docs/RUNBOOK.md", "docs/RELEASE_VALIDATION.md"):
        text = (ROOT / doc).read_text()
        assert "provision `azure-identity" not in text and "requires `azure-identity` installed" not in text, doc


def test_bundle_version_tag_matches_changelog():
    import re
    import yaml
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    tag = bundle["resources"]["jobs"]["nightly_full_refresh"]["tags"]["version"]
    latest = re.search(r"^## (\d+\.\d+\.\d+)", (ROOT / "CHANGELOG.md").read_text(), flags=re.M).group(1)
    assert tag == latest
