"""Regression coverage for the operational failures found in the v2.2 review."""
from datetime import timedelta
from pathlib import Path

import pytest

from bridge import contract, planning, process, synapse
from test_process import FakeOps, NOW, published


@pytest.fixture
def ctx(inventory):
    # The FakeOps CETAS validation uses the example inventory from test_process.
    import test_process
    test_process.SETTINGS = inventory
    return process.Context(settings=inventory, release="synapse_nightly-2026092500", sequence=2026092500,
                           business_date="2026-09-25", job_run_id="42", cutoff_utc=NOW + timedelta(hours=10))


@pytest.fixture
def m(inventory):
    return inventory["mappings"][0]


class SqlError(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(f"SQL error {code}")


@pytest.mark.parametrize("code,status", [(207, "FAILED_SCHEMA_DRIFT"), (208, "FAILED_SCHEMA_DRIFT"),
                                          (229, "FAILED_SOURCE_ACCESS"), (230, "FAILED_SOURCE_ACCESS")])
def test_deterministic_sql_errors_do_not_fall_back(ctx, m, code, status):
    ops = FakeOps(m, export_raises=SqlError(code))
    assert process.process_table(ctx, m, ops, now=NOW) == status
    assert "export" in ops.calls and "jdbc" not in ops.calls
    assert "FALLBACK_REQUIRED" not in [r["status"] for r in ops.logs]
    assert status in planning.DETERMINISTIC_FAILURES


def test_recovered_commit_count_can_finish_after_publication_cutoff(ctx, m):
    ops = FakeOps(m, history=[published(ctx.release, ctx.sequence, version=8)])
    ctx.cutoff_utc = NOW - timedelta(seconds=1)
    ctx.table_info[m["mapping_id"]] = {}
    assert process.process_table(ctx, m, ops, now=NOW) == "ALREADY_CURRENT"
    assert ops.calls == ["connect", "count"]
    assert ops.logs[-1]["written_rows"] == 10


def test_restore_to_good_version_keeps_rollback_candidate():
    good = published("good", 2026092500, version=10)
    bad = published("bad", 2026092600, version=11)
    restore = {"version": 12, "operation": "RESTORE", "userMetadata":
               planning.restore_metadata("bad", 11, 10, "automatic")}
    assert planning.rollback_candidates([restore, bad, good], {10: 10}, "2026-09-25") == [10]
    assert planning.restored_after([restore, bad, good], bad)
    assert not planning.restored_after([restore, bad, good], good)
    unknown = dict(restore, userMetadata=None)
    assert planning.rollback_candidates([unknown, bad, good], {10: 10}, "2026-09-25") == []
    external = dict(unknown, operationParameters={"version": "10"})
    assert planning.rollback_candidates([external, bad, good], {10: 10}, "2026-09-25") == [10]
    later_write = dict(bad, version=13)
    assert planning.rollback_candidates([later_write, restore, bad, good], {10: 10}, "2026-09-25") == []


@pytest.mark.parametrize("first,second", [("a-b", "a_b"), ("A_B", "a_b"),
                                                ("x" * 80 + "a", "x" * 80 + "b")])
def test_external_table_name_prefix_collision_is_rejected(inventory, first, second):
    inventory["mappings"][0]["mapping_id"] = first
    inventory["mappings"][1]["mapping_id"] = second
    with pytest.raises(ValueError, match="external-table name prefix"):
        contract.validate_inventory(inventory)


def test_setup_guards_each_batch():
    root = Path(__file__).parents[1] / "sql" / "synapse"
    batches = (root / "01_export_setup.sql").read_text().split("\nGO\n")
    assert all("Replace all placeholders" in b for b in batches[:-1])
