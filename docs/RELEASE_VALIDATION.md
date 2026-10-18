# Release validation — 4A (serverless CETAS + serverless UC-JDBC fallback)

Local validation performed after the compute split correction:

- `python -m pytest tests -q` (v2.6.9): **404 passed, 20 skipped** on a machine without PySpark, Java and `mssql-python` (the Spark, Derby and real-driver tests skip). Record the full count from the CI run, which installs all three, before approving a release.
- `python -m compileall -q src`: **passed**.
- `databricks.yml` parses successfully and compute-placement regression tests assert that every nightly and acceptance task runs on the serverless environment and that only the operator maintenance and rollback jobs use classic job clusters.
- Actual serverless-to-Synapse network reachability, `mssql-python` package installation, Synapse permissions, CETAS execution, UC-JDBC connectivity and read options, and Delta acceptance behavior must be proven in the real DEV workspace before PROD.

Do not treat local tests as proof of cloud networking or permissions. Run `databricks bundle validate -t dev`, deploy to DEV, and execute the acceptance job plus representative CETAS-success and forced-fallback tables.

## Environment checks (run in DEV, then again in PROD before go-live)

Local tests cannot prove these. Complete each one in DEV before approving the prod
deployment, then repeat them in PROD while the schedule is still paused.

- `databricks bundle validate -t <env>` succeeds (the deploy workflow also runs it).
- The acceptance job passes on serverless. It proves the Spark confs the nightly sets
  (including the Parquet rebase-mode confs), idempotent `INSERT OVERWRITE`, RESTORE,
  governance preflight, the UC-JDBC read options and the `mssql-python` CETAS command path.
- `sql/synapse/03_review_credential_scope.sql` has been reviewed and the credential reach
  approved before the CETAS permissions are granted.
- A nightly run with `only_mappings` set to one CETAS-success table publishes it
  (`PUBLISHED`), and grants on the target survive the overwrite.
- A forced-fallback table reaches `FALLBACK_REQUIRED`, then `VALIDATED`, then `PUBLISHED`
  through the serverless UC-JDBC task, including a partitioned read when
  `jdbc_partition_column` is set.
- `90_finalize` delivers one notification for the run.
