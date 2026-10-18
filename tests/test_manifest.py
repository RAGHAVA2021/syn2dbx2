"""v1.4: the upstream row-count manifest."""
import json
from datetime import date, datetime, timezone

import pytest

from bridge import contract, manifest

HEADER = "business_date,upstream_run_id,upstream_sequence,schema,table,row_count\n"


def csv_text(*rows, header=HEADER):
    return header + "".join(r + "\n" for r in rows)


def ok_rows():
    return ("2026-09-25,UP1,2026092500,dbo,Customer,1204331",
            "2026-09-25,UP1,2026092500,dbo,SalesOrder,58412907")


def ids(inventory):
    return {m["source_object"]: m["mapping_id"] for m in inventory["mappings"]}


def test_happy_path_counts_every_mapped_table(inventory):
    mf = manifest.parse(csv_text(*ok_rows()), inventory, inventory["mappings"], expected_business_date="2026-09-25")
    i = ids(inventory)
    assert mf.counts == {i["Customer"]: 1204331, i["SalesOrder"]: 58412907}
    assert (mf.business_date, mf.upstream_run_id, mf.sequence, mf.warnings) == ("2026-09-25", "UP1", 2026092500, [])


def test_bom_crlf_blank_lines_and_case_insensitive_names(inventory):
    text = "\ufeff" + HEADER.replace("\n", "\r\n") + "2026-09-25,UP1,2026092500,DBO,customer,5\r\n\r\n"
    mf = manifest.parse(text, inventory, inventory["mappings"])
    assert mf.counts == {ids(inventory)["Customer"]: 5}


def test_quoted_names_are_parsed_as_csv(inventory):
    inventory["mappings"][0]["source_object"] = "Customer"
    text = csv_text('2026-09-25,"UP,1",2026092500,"dbo","Customer",7')
    mf = manifest.parse(text, inventory, inventory["mappings"])
    assert mf.upstream_run_id == "UP,1" and list(mf.counts.values()) == [7]


def test_custom_header_names_and_delimiter(inventory):
    inventory["manifest"]["columns"] = {"schema": "SchemaName", "table": "TableName", "row_count": "Rows",
                                        "business_date": "LoadDate", "upstream_run_id": "RunId",
                                        "upstream_sequence": "Seq"}
    inventory["manifest"]["delimiter"] = "|"
    text = "LoadDate|RunId|Seq|SchemaName|TableName|Rows\n2026-09-25|R9|2026092501|dbo|Customer|3\n"
    mf = manifest.parse(text, inventory, inventory["mappings"])
    assert mf.counts == {ids(inventory)["Customer"]: 3} and mf.sequence == 2026092501 and mf.upstream_run_id == "R9"


def test_sequence_is_derived_when_the_column_is_absent(inventory):
    text = "business_date,upstream_run_id,schema,table,row_count\n2026-09-25,UP1,dbo,Customer,1\n"
    assert manifest.parse(text, inventory, inventory["mappings"]).sequence == 2026092500


@pytest.mark.parametrize("text,message", [
    ("business_date,schema,table\n2026-09-25,dbo,Customer\n", "missing column"),
    (HEADER, "no data rows"),
    (csv_text("2026-09-25,UP1,2026092500,dbo,Customer,1", "2026-09-24,UP1,2026092500,dbo,SalesOrder,2"),
     "mixes business dates"),
    (csv_text("2026-09-25,UP1,2026092500,dbo,Customer,1", "2026-09-25,UP2,2026092500,dbo,SalesOrder,2"),
     "one non-empty upstream_run_id"),
    (csv_text("2026-09-25,UP1,2026092400,dbo,Customer,1"), "yyyymmdd followed by a 2-digit revision"),
    (csv_text("2026-09-25,UP1,abc,dbo,Customer,1"), "yyyymmdd followed by a 2-digit revision"),
    (csv_text("25/09/2026,UP1,2026092500,dbo,Customer,1"), "not yyyy-mm-dd"),
])
def test_whole_file_problems_raise(inventory, text, message):
    with pytest.raises(manifest.ManifestError, match=message):
        manifest.parse(text, inventory, inventory["mappings"])


def test_wrong_night_or_run_is_refused(inventory):
    with pytest.raises(manifest.ManifestError, match="expected 2026-09-26"):
        manifest.parse(csv_text(*ok_rows()), inventory, inventory["mappings"], expected_business_date="2026-09-26")
    with pytest.raises(manifest.ManifestError, match="expected UP7"):
        manifest.parse(csv_text(*ok_rows()), inventory, inventory["mappings"], expected_run_id="UP7")


def test_run_id_required_when_manifest_is_the_completion_signal(inventory):
    text = "business_date,schema,table,row_count\n2026-09-25,dbo,Customer,1\n"
    manifest.parse(text, inventory, inventory["mappings"])                       # fine for counts only
    with pytest.raises(manifest.ManifestError, match="required when it is the completion signal"):
        manifest.parse(text, inventory, inventory["mappings"], require_run_id=True)


def test_bad_rows_only_drop_that_table(inventory):
    text = csv_text("2026-09-25,UP1,2026092500,dbo,Customer,12x",
                    "2026-09-25,UP1,2026092500,dbo,SalesOrder,10",
                    "2026-09-25,UP1,2026092500,dbo,SalesOrder,10",       # identical duplicate: fine
                    "2026-09-25,UP1,2026092500,dbo,Unmapped,4",
                    "2026-09-25,UP1,2026092500,dbo,Other,-1")
    mf = manifest.parse(text, inventory, inventory["mappings"])
    assert mf.counts == {ids(inventory)["SalesOrder"]: 10}
    assert any("not a whole number" in w for w in mf.warnings)
    assert any("2 manifest row(s)" in w for w in mf.warnings)


def test_conflicting_duplicates_drop_the_count(inventory):
    text = csv_text("2026-09-25,UP1,2026092500,dbo,Customer,1", "2026-09-25,UP1,2026092500,dbo,Customer,2")
    mf = manifest.parse(text, inventory, inventory["mappings"])
    assert mf.counts == {} and "conflicting counts" in mf.warnings[0]


def test_one_source_mapped_to_two_targets_receives_count_for_both(inventory):
    import copy
    second = copy.deepcopy(inventory["mappings"][0])
    second["mapping_id"] = "customer_copy"
    second["target_table"] = "customer_copy"
    inventory["mappings"].append(second)
    mf = manifest.parse(csv_text(ok_rows()[0]), inventory, inventory["mappings"])
    assert mf.counts == {inventory["mappings"][0]["mapping_id"]: 1204331, "customer_copy": 1204331}


def test_summary(inventory):
    mf = manifest.parse(csv_text(ok_rows()[0]), inventory, inventory["mappings"])
    assert mf.summary([m["mapping_id"] for m in inventory["mappings"]]) == \
        {"tables_with_counts": 1, "tables_without_counts": 1, "warnings": 0}


def test_marker_must_certify_exact_manifest_generation(inventory):
    mf = manifest.parse(csv_text(ok_rows()[0]), inventory, inventory["mappings"])
    marker = {"business_date": mf.business_date, "upstream_run_id": mf.upstream_run_id,
              "upstream_sequence": mf.sequence}
    assert manifest.marker_matches(json.dumps(marker), mf)
    assert not manifest.marker_matches("", mf)
    assert not manifest.marker_matches(json.dumps({**marker, "upstream_run_id": "old"}), mf)
    assert not manifest.marker_matches(json.dumps({**marker, "upstream_sequence": mf.sequence-1}), mf)


# ---- paths, dates, settings ----------------------------------------------------------

def test_paths_for_both_placeholders(inventory):
    root = "abfss://upstream@contosocontrol.dfs.core.windows.net"
    assert manifest.path_for(inventory, "2026-09-25") == f"{root}/upstream-manifests/2026-09-25/row_counts.csv"
    assert manifest.marker_path(inventory, date(2026, 9, 25)) == f"{root}/upstream-manifests/2026-09-25/_SUCCESS"
    inventory["manifest"]["path_template"] = f"{root}/m/rc_{{business_date_compact}}.csv"
    assert manifest.path_for(inventory, "2026-09-25") == f"{root}/m/rc_20260925.csv"
    inventory["manifest"]["success_marker"] = None
    assert manifest.marker_path(inventory, "2026-09-25") is None


@pytest.mark.parametrize("change,message", [
    (lambda d: d["manifest"].update(path_template="https://x/y/{business_date}.csv"), "path_template"),
    (lambda d: d["manifest"].update(path_template=d["staging"]["root"] + "/m/rc.csv"), "path_template"),
    (lambda d: d["manifest"].update(success_marker=None), "success_marker is required"),
    (lambda d: d["manifest"].update(columns={"bogus": "x"}), "may only rename"),
    (lambda d: d["manifest"].update(columns={"schema": "table"}), "distinct header names"),
    (lambda d: d["manifest"].update(delimiter="||"), "single character"),
    (lambda d: d.update(upstream={"completion_source": "email"}), "completion_source"),
    (lambda d: d.pop("manifest"), "no manifest settings"),
])
def test_settings_validation(inventory, change, message):
    change(inventory)
    with pytest.raises(ValueError, match=message):
        contract.validate_inventory(inventory)


def test_manifest_is_optional_in_synapse_table_mode(inventory):
    inventory["upstream"] = {"completion_source": "synapse_table"}
    inventory["synapse"]["upstream_control_schema"] = "bridge_upstream"
    inventory.pop("manifest")
    contract.validate_inventory(inventory)
    inventory["manifest"] = {"path_template": "abfss://upstream@contosocontrol.dfs.core.windows.net/m/{business_date}.csv",
                             "success_marker": None}
    contract.validate_inventory(inventory)           # counts only: the marker is optional


def test_manifest_cannot_share_job_writable_staging(inventory):
    inventory["manifest"]["path_template"] = inventory["staging"]["root"] + "/m/{business_date}.csv"
    with pytest.raises(ValueError, match="separate read-only container"):
        contract.validate_inventory(inventory)


def test_hidden_manifest_file_name_is_rejected(inventory):
    doc = dict(inventory)
    doc["manifest"] = {**inventory["manifest"],
                       "path_template": inventory["manifest"]["path_template"].rsplit("/", 1)[0] + "/_counts.csv"}
    problems = []
    manifest.validate_settings(doc, problems)
    assert any("must not start with '_'" in p for p in problems)


def test_shipped_examples_certify_each_other(inventory):
    """The files handed to the upstream team must pass the real parser and marker check."""
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    mappings = [dict(inventory["mappings"][0], source_schema="dbo", source_object="Customer", mapping_id="customer")]
    parsed = manifest.parse((root / "config" / "row_counts.example.csv").read_text(), inventory, mappings,
                            expected_business_date="2026-09-25", require_run_id=True)
    assert parsed.counts == {"customer": 1204331}
    assert manifest.marker_matches((root / "config" / "success_marker.example.json").read_text(), parsed)
