"""Generate config/inventory.json from the Synapse catalog.

Reads every table/view in the given source schemas, records column metadata and
the schema hash, and maps types with the same contract the job uses. Columns
the automatic contract cannot handle (MAX text, binary, xml, spatial, ...) are
reported together and no inventory is written, so nothing silently drops.

Run from a machine with the Microsoft ODBC Driver 18 and network access to the
pool:

  export BRIDGE_SYNAPSE_ODBC='Driver={ODBC Driver 18 for SQL Server};Server=tcp:<ws>.sql.azuresynapse.net,1433;Database=<pool>;Encrypt=yes;Authentication=ActiveDirectoryInteractive;'
  python tools/generate_inventory.py --settings config/settings.json \
      --source-schema dbo --target-catalog analytics --target-schema gold \
      --output config/inventory.json [--profile] [--exclude-file exclude.txt]
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bridge import contract  # noqa: E402

LIST_OBJECTS_SQL = (
    "SELECT o.name, o.type FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id "
    "WHERE s.name=? AND o.type IN ('U','V') AND o.is_ms_shipped=0 ORDER BY o.name")

# Dedicated-pool storage size per table (views have no storage: size 0).
PROFILE_SQL = (
    "SELECT SUM(nps.used_page_count) * 8192 FROM sys.tables t "
    "JOIN sys.schemas s ON s.schema_id=t.schema_id "
    "JOIN sys.pdw_table_mappings tm ON tm.object_id=t.object_id "
    "JOIN sys.pdw_nodes_tables nt ON nt.name=tm.physical_name "
    "JOIN sys.dm_pdw_nodes_db_partition_stats nps ON nps.object_id=nt.object_id "
    "AND nps.pdw_node_id=nt.pdw_node_id AND nps.distribution_id=nt.distribution_id "
    "WHERE s.name=? AND t.name=?")


def mapping_id_for(schema, obj):
    mid = f"{schema}__{obj}"
    if not contract.KEY.fullmatch(mid):
        raise ValueError(f"{schema}.{obj}: cannot derive a safe mapping_id")
    return mid


def build_mapping(schema, obj, sys_rows, target_catalog, target_schema, estimated_bytes=0):
    """Pure: one inventory mapping from sys.columns rows. Raises UnsupportedType."""
    cols = contract.columns_from_sys_rows(sys_rows)
    problems = []
    for c in cols:
        try:
            contract.column_contract(c)
        except (contract.UnsupportedType, ValueError) as e:
            problems.append(str(e))
    if problems:
        raise contract.UnsupportedType(f"{schema}.{obj}: " + "; ".join(problems))
    return {
        "mapping_id": mapping_id_for(schema, obj),
        "enabled": True,
        "source_schema": schema, "source_object": obj,
        "target_catalog": target_catalog, "target_schema": target_schema, "target_table": obj.lower(),
        "mandatory": False, "allow_empty": False, "verify_source_count": True,
        "estimated_bytes": int(estimated_bytes or 0),
        "source_schema_hash": contract.schema_hash(sys_rows),
        "columns": [{k: c[k] for k in ("name", "sql_type", "max_length", "precision", "scale", "nullable")}
                    for c in cols],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--settings", required=True, help="JSON with every top-level key except mappings")
    ap.add_argument("--source-schema", action="append", required=True)
    ap.add_argument("--target-catalog", required=True)
    ap.add_argument("--target-schema", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--profile", action="store_true", help="record table sizes for batching")
    ap.add_argument("--exclude-file", help="one schema.object per line to skip")
    a = ap.parse_args()
    if os.path.exists(a.output):
        raise SystemExit(f"{a.output} exists; refusing to overwrite a reviewed inventory")
    settings = json.loads(Path(a.settings).read_text())
    excluded = set()
    if a.exclude_file:
        excluded = {l.strip().lower() for l in Path(a.exclude_file).read_text().splitlines() if l.strip()}

    import pyodbc
    conn = pyodbc.connect(os.environ["BRIDGE_SYNAPSE_ODBC"], autocommit=True)
    mappings, errors, views = [], [], []
    for schema in a.source_schema:
        contract.ident(schema, "source schema")
        for name, kind in conn.cursor().execute(LIST_OBJECTS_SQL, schema).fetchall():
            if f"{schema}.{name}".lower() in excluded:
                continue
            try:
                contract.ident(name, "source object")
                rows = conn.cursor().execute(contract.SYS_COLUMNS_SQL, f"[{schema}].[{name}]").fetchall()
                size = 0
                if a.profile and kind.strip() == "U":
                    size = conn.cursor().execute(PROFILE_SQL, schema, name).fetchone()[0] or 0
                mappings.append(build_mapping(schema, name, rows, a.target_catalog, a.target_schema, size))
                if kind.strip() == "V":
                    views.append(mappings[-1]["mapping_id"])
            except (contract.UnsupportedType, ValueError) as e:
                errors.append(str(e))
    if errors:
        print("No inventory written. Fix, cast in a reviewed view, or exclude these objects:", file=sys.stderr)
        for e in errors:
            print("  " + e, file=sys.stderr)
        raise SystemExit(1)
    doc = {**settings, "mappings": mappings}
    contract.validate_inventory(doc)
    Path(a.output).write_text(json.dumps(doc, indent=2) + "\n")
    print(f"Wrote {len(mappings)} mappings to {a.output}. Review mandatory/allow_empty flags before deploying.")
    if views:
        # Batching is biggest-first; a view has no storage size, so it would be
        # scheduled last even when it is one of the heaviest exports.
        print(f"{len(views)} view mapping(s) have estimated_bytes 0 and will be scheduled last. Set "
              "estimated_bytes by hand for heavy views (e.g. the size of their main base table): "
              + ", ".join(views[:20]) + (" ..." if len(views) > 20 else ""), file=sys.stderr)


if __name__ == "__main__":
    main()
