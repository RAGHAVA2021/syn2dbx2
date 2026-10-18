# Changelog — 4A — serverless CETAS + serverless JDBC fallback

## 2.6.9
## 2026-10-12 fresh-install cleanup: one fallback route, manifest-only completion

Nothing has been installed yet, so upgrade paths and unused alternatives were removed.

- **One JDBC fallback route.** The in-task Spark-JDBC fallback is removed. A CETAS-specific
  failure or a pre-1900 timestamp always ends `FALLBACK_REQUIRED` and is staged by the serverless
  UC-JDBC task (`20_serverless_jdbc_fallback`). Removed: `execution.defer_jdbc_fallback` (an old
  inventory that still sets it is accepted; the key is ignored), `loader.jdbc_export`,
  `contract.jdbc_query`, `synapse.partition_bounds`, `synapse.DRIVER`. The Parquet staging of
  `loader.uc_jdbc_export` is now `loader.stage_jdbc_query`, which the Derby tests exercise directly.
- **Manifest completion only.** `contract.variant_4a_problems` (deploy, UC setup, acceptance) refuses
  any `upstream.completion_source` other than `"manifest"`, including an unset one (the code default
  is `synapse_table`). `synapse.upstream_control_schema` is required only in `synapse_table` mode and
  is removed from the example configs. The shared `synapse_table` code path remains but is unreachable.
- **Removed files:** `sql/synapse/02_upstream_control.sql` (and the `bridge_upstream` schema and grant in
  `01_export_setup.sql`), `sql/unity_catalog/upgrade_*.sql`, `src/notebooks/10_process_batch.py`,
  `FIRST_READ_ME.md`, `docs/V22_REVIEW_RESOLUTION.md`, `docs/COMPUTE_DESIGN.md` (its unique content
  moved to DESIGN 3.4). `render_uc_setup.py` no longer prints upgrade-script hints.
- **New:** `docs/UPSTREAM_MANIFEST_SPEC.md` (file contract for the upstream team) and
  `config/success_marker.example.json`; a test proves the two examples certify each other.
- **Documentation:** all docs describe the 4A task chain (extract → UC-JDBC fallback → publish);
  PACKAGE_GUIDE lists who uses each file, when, and in what order; README and PACKAGE_GUIDE give the
  reading order. Stale cluster/JVM wording removed.

**Inventory actions:** set `upstream.completion_source = "manifest"` and the `upstream_wait_minutes`
you want (the code default is 0, no wait). `execution.defer_jdbc_fallback` can be deleted.

## 2.6.8
## 2026-09-27 deploy workflow and documentation fixes

No runtime change: the notebooks, `src/bridge` and the job definitions behave exactly as in 2.6.7
(only the bundle's version tag changes).

- **Deploy workflow:** `bundle-deploy.yml` pins `databricks/setup-cli` to the v1.18.0 commit instead
  of `@main`, so a change to the action cannot alter a prod deploy, and runs Python 3.12 like CI and
  serverless environment version 3 (was 3.11).
- **RUNBOOK step 13 pointed at a section that did not exist** ("Additional for v2.6.5" in
  `RELEASE_VALIDATION.md`). `RELEASE_VALIDATION.md` now has an "Environment checks" section listing the
  DEV/PROD gates, and the RUNBOOK refers to it.
- `RELEASE_VALIDATION.md` drops the stale 4A/4B title and "serverless/dedicated boundaries" wording.
- `tests/test_v268_regressions.py` guards all of the above.

## 2.6.7
## 2026-09-27 independent review fixes

- **Bundle:** removed the duplicate `environment_key` on the fallback task (strict YAML
  loaders, including the Databricks CLI's, reject duplicate keys); a strict-YAML test guards it.
  Removed the unused `max_workers` variable. CI tests on Python 3.12 like serverless environment 3.
- **CETAS-error fallbacks were counted as timestamp reroutes.** mssql-python keeps the SQLSTATE but
  not the error number, so most CETAS failures reached the deferred fallback with `error_code` NULL,
  and `fallback_alert_threshold` could never fire. Such a failure is now logged with `error_code = 0`
  (`process.CETAS_ERROR_WITHOUT_NUMBER`); NULL still means a historical-timestamp reroute. The detail
  names the SQLSTATE instead of "error None".
- **Data errors went to the JDBC fallback.** A server error with SQLSTATE class 22 (data exception)
  and no recognized number is now `DATA` (`FAILED_SOURCE_DATA`), per invariant 10. *Owner review:*
  this is a SQLSTATE-class rule, not a new error code; confirm it on the trial night.
- **Fallback outcomes use the real statuses.** A failed UC-JDBC attempt logs `FAILED_EXPORT`,
  `FAILED_COUNT_MISMATCH` or `FAILED_UNKNOWN` with `method = JDBC` (was the in-flight
  `FALLBACK_REQUIRED`), so when both routes fail the summary shows `FAILED_EXPORT` as AGENTS.md
  requires. The task retry and a Repair run still re-attempt it (`control.failed_fallbacks`).
- **UC-JDBC fallback read contract.** Every read is bounded by the publication deadline
  (`queryTimeout`), so one slow fallback no longer holds every table's publication past the cutoff.
  It now honours `jdbc_partition_column`/`jdbc_num_partitions`, `jdbc_fetch_size` and
  `preferTimestampNTZ` like the classic route. `bounds_sql` names its columns (T-SQL 8155 through a
  JDBC `query`). The UC connection must allow these options; the acceptance job proves it.
- **4A settings enforced before deploy:** `tools/prepare_deploy.py` and `tools/render_uc_setup.py`
  refuse an inventory without `execution.defer_jdbc_fallback = true` and `synapse.uc_jdbc_connection`
  (`contract.variant_4a_problems`); the example inventory sets both.
- **Acceptance job** fails instead of skipping when the UC connection is missing; it runs the
  production `loader.uc_jdbc_export` (plain and partitioned) and proves the serverless CETAS command
  path: `mssql-python` installed, login, staging-object check and a one-row CETAS read back by Spark.
- Publishing from a stage reused from an earlier phase removes every attempt folder of that
  release/mapping (the CETAS folder preceding a JDBC fallback was left for the lifecycle rule).
- Publish reports a VALIDATED stage that no longer exists as such, instead of "CETAS failed (UNKNOWN)".
- A missing `mssql-python` raises a clear error naming the job environment.

**Open design decision (not changed):** the phases are barriers: no table publishes until every
extract batch and every fallback batch has finished. The deadline-bounded fallback limits the damage,
but a slow extraction tail still delays every publication. Publishing per batch would need a reviewed
job-graph change (see DESIGN 3.4).

## 2.6.6
## 2026-09-27 review fixes

- **Publish could never publish.** `30_publish_batch` passed `already={}`, so every table tried to run
  CETAS (forbidden there) and ended `FAILED_EXPORT`; its retry then skipped every table because the
  failed attempt was the latest event. It now reuses the VALIDATED stage and re-selects every mapping
  with a VALIDATED stage that has not reached a terminal status.
- **Serverless Synapse errors were all "CAPACITY".** mssql-python exceptions carry neither SQLSTATE nor
  native error. `sql_error_info` now recovers the SQLSTATE from the driver's own table and the SQL Server
  error number from its message text; a server SQLSTATE without a number classifies as `CETAS`. The
  plan's transient retry now recognizes dropped connections (08S01) on serverless.
- **Fallback moved to `bridge/fallback.py` (unit-tested).** It always proves the row count (UC-JDBC
  COUNT_BIG when the manifest has none, so such tables can publish), logs deterministic failures with
  their real status, retries a mapping left in `FALLBACK_STARTED`, and skips past the publish deadline.
- `count_sql` names its column (`AS row_count`): T-SQL rejects unnamed derived-table columns (8155).
- `control.fallback_counts` counts per mapping across `FALLBACK_REQUIRED`/`FALLBACK_STARTED`; the CETAS
  fallback alert could never fire in the deferred design.
- Extraction retries no longer re-run CETAS for a table already handed to the deferred fallback;
  `FALLBACK_REQUIRED` is logged once.
- Acceptance job runs on serverless and now proves every Spark conf the nightly sets, that the
  governance preflight detects a row filter and a column mask, and the UC-JDBC route (grant,
  datetime2 wall clock incl. pre-1900, COUNT query).
- **Second review:** the serverless publish could not verify a commit whose acknowledgement was lost
  (worker died after the Delta commit): it tried a forbidden Synapse recount and looped `FAILED_UNKNOWN`,
  never restoring a wrong-count commit. Counts from a VALIDATED stage are now marked proven and are never
  recounted, so such a commit verifies (`ALREADY_CURRENT`) or is rolled back automatically.
- Fallback batch isolates per-table platform failures and reports planned mappings missing from the
  inventory; CI installs `mssql-python` so the real-driver tests run; the acceptance conf probe names the
  rejected setting even when reading it fails.
- UC setup grants `USE CONNECTION`; every nightly task declares the serverless environment; the SQL
  password is brace-quoted in the ODBC connection string; publish keeps the proven count's label.

## 2.6.5
## 2026-09-27 compute correction

- Corrected the package so CETAS command/control actually runs from a **serverless Databricks task**.
- Added serverless command SQL support through `mssql-python==1.15.0`; no Spark JVM gateway is required for CETAS.
- Kept per-table deferred fallback and independent publication (`ALL_DONE`).
- Updated tests, package names and compute documentation to match the executable bundle.
- No dedicated cluster is defined in the nightly job. A UC JDBC connection is required for fallback reads.
