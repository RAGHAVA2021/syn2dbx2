# 4A — serverless CETAS + serverless JDBC fallback

Production-oriented Synapse dedicated SQL pool → ADLS Gen2 → Unity Catalog managed Delta full-refresh package for static nightly source tables.

## Compute split

| Stage | Compute | Purpose |
|---|---|---|
| Plan / Synapse checks | Serverless | Uses `mssql-python` for command-capable T-SQL connection |
| CETAS | Serverless Databricks task; CETAS executes in Synapse | Issues CETAS DDL through `mssql-python` |
| JDBC fallback | Serverless | Unity Catalog JDBC connection; only `FALLBACK_REQUIRED` tables |
| Delta publish | Serverless | Publishes only `VALIDATED` stages |
| Finalize | Serverless | Final success/failure/fallback summary |

**Flow:** `Refresh complete → serverless plan/CETAS → serverless UC-JDBC fallback only when required → serverless Delta publish → final summary`

No dedicated cluster is defined in the nightly job. A UC JDBC connection is required for fallback reads.

## Important prerequisites

- Unity Catalog enabled workspace and serverless jobs available.
- Serverless network connectivity to the Synapse SQL endpoint and ADLS.
- `mssql-python==1.15.0` is installed by the job environment for serverless command/control SQL.
- The run identity has the required Synapse CETAS permissions and storage access.
- The upstream row-count manifest CSV and its `_SUCCESS` marker are the completion signal; source tables remain static after it.
- Recovery cutoff remains 11:00 Australia/Sydney; do not rerun the previous night after the cutoff.

## Where to start

Read the documents in this order:

1. `README.md` (this file): what the package does and its prerequisites.
2. `docs/PACKAGE_GUIDE.md`: every folder and file, with purpose, who uses it, when, and in what order.
3. `docs/DESIGN.md`: how it works and why (sections 1–4 essential).
4. `AGENTS.md`: rules that must not be broken; required before changing anything.
5. `docs/UPSTREAM_MANIFEST_SPEC.md`: send to the upstream team early, with the two `config/` examples.
6. `docs/FIRST_RUN.md`: **start here to install**: every step in order, from collecting names through Synapse, secrets, settings, deploy and the first run, to prod go-live.
7. `docs/SETUP_CHECKLIST.md`: the same path as a shorter tick-list, with a map of which script runs when.
8. `infra/README.md`: storage lifecycle and identity/access checks.
9. `docs/RUNBOOK.md`: reference for installation, deployment, daily checks, recovery and rollback.
10. `docs/RELEASE_VALIDATION.md`: dev and prod checks before go-live.
11. `docs/DATA_DICTIONARY.md`: control-table columns for daily checks and investigations.
12. `docs/CODE_STRUCTURE.md`: only when changing code.
13. `CHANGELOG.md`: only when taking a new package version.

Reading paths by role (installer, upstream team, admins, operator, developer,
approver) are in `docs/PACKAGE_GUIDE.md`, "Suggested reading order".

Keep the nightly schedule PAUSED until DEV connectivity, permissions, CETAS,
fallback, publish and notification acceptance checks pass
(`docs/RELEASE_VALIDATION.md`).

Run the tests with `python -m pytest tests -q`.
