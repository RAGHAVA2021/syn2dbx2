# Agent / reviewer guidance — 4A — serverless CETAS + serverless JDBC fallback

**Authoritative compute design:** `Refresh complete → serverless plan/CETAS → serverless UC-JDBC fallback only when required → serverless Delta publish → final summary`. No dedicated cluster is defined in the nightly job. A UC JDBC connection is required for fallback reads.

## 1. Purpose in one paragraph

Every night, copy about 1,000 tables and views from an **Azure Synapse dedicated SQL
pool** into **Unity Catalog Delta tables**, in full (a full refresh, not incremental),
before a business cutoff (default 07:00 Australia/Sydney).

Each table:

* is exported once with CETAS to ADLS Parquet;
* is validated completely;
* then replaces its target's data in **one atomic `INSERT OVERWRITE`**.

A table that fails for any reason keeps serving the previous night's data. Tables
refresh independently; there is no all-tables-at-once cutover.

## 2. Environment facts (given by the owner — do not re-provision)

* **Databricks and the Synapse pool are already connected** in this environment. The
  owner currently reads Synapse tables into DataFrames with Spark's JDBC source
  (`spark.read.jdbc`/`format("jdbc")`).
  * Reuse that connection's identity. Map its authentication to `synapse.auth.mode`:

    | Existing JDBC authentication | `synapse.auth.mode` | Secret-scope keys |
    |---|---|---|
    | `user`/`password` (SQL login) | `sql_password` | `secret_scope`, `user_key`, `password_key` |
    | Service principal (client id + secret) | `client_secret` | `secret_scope`, `tenant_id_key`, `client_id_key`, `client_secret_key` |
    | Managed identity via an access connector | `service_credential` | `credential_name` (a UC service credential) |

  * All three modes feed `synapse.credentials_provider`, which serves both the command
    connection (CETAS) and the JDBC fallback. Never put secrets in the inventory; only
    secret-scope names and keys.
  * Reuse the existing identity, credentials and storage objects.
  * Do **not** create new service principals, secret scopes, UC connections, storage
    credentials, external locations, Synapse credentials, external data sources or
    file formats unless a human explicitly asks.
  * Instead, find the existing names and put them into `config/inventory.json`:
    * `synapse.auth` — the identity mode and its credential/secret references;
    * `staging.external_data_source` and `staging.file_format` — the Synapse-side objects;
    * `staging.root` — the container Databricks reads through an existing external location.
* **Check that the existing connection can do what this design needs.** Reading tables
  over JDBC needs only SELECT; this design additionally **executes DDL (`CREATE EXTERNAL
  TABLE AS SELECT`) on the pool** and writes fallback files to staging. The existing
  connection must provide all of the following:

  | # | Required | How to check |
  |---|---|---|
  | 1 | Credentials for the pool | One of the three modes above, served by `synapse.credentials_provider` |
  | 2 | The CETAS permissions listed in `sql/synapse/01_export_setup.sql` | Query effective permissions for the job identity on the pool |
  | 3 | A Synapse external data source that writes to the staging container | `SELECT name, location FROM sys.external_data_sources` |
  | 4 | Databricks READ FILES **and WRITE FILES** on that container (WRITE is for the JDBC fallback's staged Parquet) | `SHOW EXTERNAL LOCATIONS` and `SHOW GRANTS ON EXTERNAL LOCATION <name>` |
  | 5 | The reused external file format is **PARQUET**, and the data source location equals `staging.root` | Enforced automatically: the plan step runs `synapse.check_staging_objects` and fails the night before any table if either is wrong |

* **Security gate before granting CETAS rights:** `ALTER ANY EXTERNAL DATA SOURCE`
  reaches all database-scoped credentials in this database. Run the read-only
  `sql/synapse/03_review_credential_scope.sql` as an administrator and obtain
  approval for the complete credential reach or isolate/redesign the export.
  Review again when credentials are added. A schema grant does not contain it.
* **Exclusive target writer gate:** automatic RESTORE is not conditional on the
  expected Delta version. Do not run a second target writer, including manual
  rollback, while a nightly task can commit or restore the same target. The
  post-RESTORE history check detects an unexpected outcome but cannot close the
  gap before the RESTORE command.

* **A Lakehouse Federation connection is not enough on its own.** Federation is
  read-only query pushdown; it cannot run CETAS. If the only link is a UC *connection*
  used for federation, stop and tell the human: the export needs an identity that can
  log in over JDBC with the permissions above. Do not replace CETAS with a federated
  `SELECT` into Delta without approval. That changes the performance and correctness
  model (see decision D3).
* **The upstream publishes a row-count manifest CSV in Azure storage** (added in 1.4).
  * It is configured under `manifest` in the inventory (path template, success marker,
    header names, delimiter).
  * The job reads it instead of scanning every table with `COUNT_BIG`.
  * With `upstream.completion_source = "manifest"`, the manifest plus its success marker
    is also the "upstream finished" signal, so `bridge_upstream.upstream_run` is not needed.
    **4A uses manifest completion only:** `contract.variant_4a_problems` refuses any other
    mode before deploy, and the Synapse completion-table script is not shipped. The
    `synapse_table` code path stays in the shared modules and their tests.
  * In manifest completion mode the marker contains JSON for the CSV's exact
    business date, run id and sequence. The upstream removes the old marker
    before any same-day correction begins and writes the new marker last.
  * The upstream must also wait for the prior bridge generation to finalize
    (`FINISHED` or `FINISHED_WITH_FAILURES`) and for active exports to stop,
    or obtain an explicit operator release after a failed job, before a
    same-day correction. Removing the marker does not stop
    an export already running. Dedicated pools default to READ UNCOMMITTED;
    RCSI may prevent dirty rows within a statement but does not fence writes
    across all mapped tables or JDBC partitions. The scheduler owns the fence.
  * The job identity needs READ FILES on the manifest's location.
* If an existing object has a different name from the defaults (`bridge_stage`,
  `bridge_parquet`, `bridge_meta`), **change the inventory settings, not the object**.

## 3. Invariants — never break these

1. **Consumers never see a partial table.** The only write to a target is a single
   `INSERT OVERWRITE` commit (`loader.overwrite`). No appends, no DELETE+INSERT, no
   multi-step writes, no DROP/CREATE of targets. The overwrite always replaces every
   partition: a bridge commit that Delta records as `isDynamicPartitionOverwrite`
   (Runtime 18 history) fails verification (`planning.dynamic_partition_overwrite`).
2. **Nothing is written until every check passes.** The order in
   `process.process_table` is deliberate: target schema → history precheck → source
   schema hash → optional source count → export (CETAS, or the JDBC fallback) → Parquet
   validation → overwrite → verify. Do not move the overwrite earlier or skip a check to
   save time.
3. **An older night can never overwrite a newer one**, and a retry can never double-load.
   This rests entirely on the Delta idempotent-write settings:
   * `txnAppId = "bridge:<mapping_id>"` — constant per table, never include a date or
     attempt in it;
   * `txnVersion = upstream_sequence` — strictly increasing per `source_set_id`.

   Do not change either definition without a migration plan.
   A target's `mapping_id` and `source_set_id` are deployment identities. Do not reuse
   the same mapping ID with a reset or independently numbered source set: the Delta
   transaction guard persists across deployments. Treat a source-set change as an
   explicit data migration, with a checked new sequence namespace.
4. **Outcome comes from Delta history, never from "the write didn't throw".** A skipped
   write returns normally. Always classify with `planning.verify_after_write`.
   **An automatic RESTORE only undoes a commit made by this same job run** (added in
   2.5). When an existing commit for the release is rechecked, the evidence order is:
   a logged verified publication of exactly that Delta version and row metric, then
   the manifest count, then a Synapse count now. A live count says nothing reliable
   about a commit left by another job run, because the source may have changed since
   that run finished; a disagreement is reported as `FAILED_VERIFY` and never restored
   automatically (`process._verify_existing_commit`).
5. **The job never changes a target's schema.** Tables are created by the rendered setup
   SQL, and schema changes are reviewed `ALTER TABLE`s. Never add `mergeSchema`,
   `overwriteSchema`, or auto-create to the loader.
6. **SQL text is built only from validated identifiers.** Export expressions are
   *derived* from column metadata at runtime (`contract.column_contract`); they are never
   stored in or read from the inventory. Every identifier passes `contract.ident`.
   Keep it that way.
7. **Deterministic failures are final within a job run; transient ones are retried once.**
   A new job run of the same release reassesses a repaired deterministic failure.
   Read prior statuses by both release id and job run id; never reuse another job's
   terminal result to skip a fresh run.
   The status sets are defined only in `planning.py`. Add new statuses there, never
   ad hoc.
8. **The control log is append-only.** Concurrent batches rely on blind appends not
   conflicting. Never UPDATE or MERGE `table_log` or `run_log`; derive current state
   with views.
9. **Datetime meaning is preserved.** SQL `datetime`/`datetime2`/`smalldatetime` become
   `timestamp_ntz` (wall clock); `datetimeoffset` becomes a UTC `timestamp`. The
   Spark session time zone must stay UTC. Changing this silently shifts values for
   every consumer.
10. **The JDBC fallback is a fallback, never a shortcut.**
    * It runs when `synapse.classify_export_error` returns `CETAS`, or when CETAS
      succeeded but validation detected a pre-1900 timestamp that may shift on
      a non-Spark INT96 read. The latter uses Spark-written Parquet with writer metadata.
    * It never runs for `DATA` errors (the same `SELECT` would fail the same way) or
      `CAPACITY` errors (a second route competes for the same pool).
    * Its output is staged as Parquet in its own `-jdbc/` folder and must pass exactly
      the same validation as CETAS output.
    * A pre-1900 timestamp reroute is a predictable data-contract fallback and
      does not consume the CETAS-error fallback budget. It still costs an extra
      Synapse read; measure its frequency before go-live.
    * The finalize threshold counts only CETAS-error fallbacks. Timestamp
      reroutes are measured separately and must not make a successful night
      fail solely because historical sentinels exist.
    * Never load a JDBC DataFrame straight into the target.
    * A missing stage is a failure, even for an allow-empty mapping; JDBC stages
      require Spark's `_SUCCESS` marker before they are read.
11. **An automatic run never publishes a stale night.** With no `business_date`, the plan
    requires a COMPLETE upstream run for today's Sydney business date. It waits up to
    `upstream_wait_minutes` or the fixed 07:00 deadline, whichever is sooner, then
    fails loudly. Never select yesterday on an automatic run: every table may return
    `ALREADY_CURRENT` while nothing refreshed.
    A manual nightly repair is limited to today's Sydney business date and a
    fixed 11:00 Sydney recovery cutoff. After that, wait for the next night.
12. **Manifest counts are trusted only by their content, never by file timestamps**
    (added in 1.4).
    * A manifest is used only if the business date, and the run id when present, *inside*
      the file match the night being published. Never use blob last-modified time:
      storage timestamps are UTC, and a same-day partial rerun rewrites the file.
    * A manifest count only replaces the `COUNT_BIG`; it never skips any validation.
    * A zero manifest count for a table that forbids empty data must be confirmed
      with one Synapse count before rejecting that table.
    * When a manifest count disagrees with the export, exactly one Synapse count
      decides: it confirms the export (publish, logged as
      `synapse_after_manifest_mismatch`) or refuses it (`FAILED_COUNT_MISMATCH`).
    * A bad manifest row only drops that table's count. A whole-file problem fails the
      plan in `manifest` completion mode, and falls back to Synapse counts in
      `synapse_table` mode.
13. **Tests stay green.** Run `python -m pytest tests -q` after any change. Changes to the
    Delta interaction also require re-running `src/notebooks/acceptance_checks.py` on the
    workspace.

## 4. Architecture map (where each responsibility lives)

| File | Responsibility | Touch when |
|---|---|---|
| `src/bridge/contract.py` | Type mapping, inventory validation, all SQL text builders, identifier safety | Adding a type, a setting, or changing export SQL |
| `src/bridge/planning.py` | Status sets, batching, cutoff, Delta-history decisions (precheck, verify, rollback target), run summary | Changing retry/finality rules or outcome classification |
| `src/bridge/process.py` | The per-table flow and batch loop; platform calls are injected via an `ops` object | Changing the order of steps (rarely; see invariant 2) |
| `src/bridge/loader.py` | UC-JDBC fallback export (staged as Parquet), reading and validating the staged Parquet, guarded overwrite, history, restore | Changing Spark-side validation or the write |
| `src/bridge/synapse.py` | Credentials (three modes), JDBC command connection, upstream lookup, schema hash, count, CETAS, partition bounds, error classification | Changing how the pool is reached |
| `src/bridge/control.py` | Append-only control log and plan tables | Adding log fields (also update `tools/render_uc_setup.py`) |
| `src/bridge/manifest.py` | Upstream row-count manifest: settings validation, path/date resolution, CSV parsing and trust rules | Changing the manifest format or trust rules |
| `src/bridge/notification.py` | Complete post-batch summary and secret-backed HTTPS delivery to the operator workflow | Changing summary fields or delivery rules |
| `src/notebooks/00_plan.py` | Finds today's Sydney upstream manifest and `_SUCCESS` marker before the 07:00 deadline, reads manifest counts, checks all source schemas with one catalog query, writes the plan (with expected rows and schema hash per table), emits batch ids | — |
| `src/notebooks/10_extract_validate_batch.py` | Serverless CETAS + validation (`validate_only`); CETAS limitations end `FALLBACK_REQUIRED`; raises only for transient failures | — |
| `src/notebooks/20_serverless_jdbc_fallback.py` | Wires `bridge/fallback.py`: UC-JDBC read bounded by the publication deadline → staged Parquet → `VALIDATED` | — |
| `src/notebooks/30_publish_batch.py` | Publishes only `VALIDATED` stages (no Synapse access); overwrite and history verification | — |
| `src/notebooks/90_finalize.py` | Summary; records the run and sends one structured notification even for optional failures below the job threshold; fails for mandatory failures, `fail_on_any_failure`, late commits, failure ratio or CETAS-error fallback alert | — |
| `src/notebooks/rollback.py`, `maintenance.py`, `acceptance_checks.py` | Operator tools | — |
| `databricks.yml` | All jobs (Asset Bundle) | Cluster, schedule, concurrency |
| `tools/generate_inventory.py` | Builds the inventory from the Synapse catalog | — |
| `tools/render_uc_setup.py` | UC DDL and grants from the inventory | New tables or log fields |
| `sql/synapse/*.sql` | Credential-scope review and pool-side export objects and grants | Only if the existing objects are missing |
| `tools/prepare_deploy.py` | Validates and selects `config/inventory.<env>.json` before a deploy | Adding an environment |
| `.github/workflows/ci-cd.yml`, `.github/workflows/bundle-deploy.yml` | Tests on pull requests; dev then approved prod deploy via GitHub OIDC | Changing the release process |
| `tests/` | Unit, flow (fake ops) and local-Spark tests | Every change |

**Runtime data flow for this variant**

1. Upstream manifest + `_SUCCESS` → **plan** on serverless (command SQL through `mssql-python`).
2. → per batch on serverless: submit CETAS through `mssql-python`, then validate the staged Parquet. CETAS success logs `VALIDATED`; a qualifying CETAS limitation logs `FALLBACK_REQUIRED`.
3. → `FALLBACK_REQUIRED` tables only: serverless Unity Catalog JDBC read → immutable Parquet → `VALIDATED`.
4. → serverless publish reuses `VALIDATED` stages → `INSERT OVERWRITE` target → Delta-history verification.
5. → **finalize** on serverless.

## 5. Design decisions (with rejected alternatives)

Treat these as settled unless a human reopens them. If you believe one is wrong, write
up the reason; do not quietly re-architect.

**D1. Overwrite the target in place, instead of candidate + snapshot + view + pointer (the v6 design).**
* *Why:* the atomic swap, Delta RESTORE, and preserved grants come with one
  Delta write per table per night. Governed targets are not supported by this
  overwrite path and fail before extraction.
* *Rejected:* v6 wrote each row twice, kept about 40–55 copies, and needed its own
  pointer protocol, leases and recovery commands.

**D2. The Delta idempotent-write guard, instead of leases, high-water marks and fencing tokens.**
* *Why:* one well-defined mechanism covers retries, lost acknowledgements, duplicate
  runs, and stuck older jobs finishing late.
* *Proof required:* `acceptance_checks.py`.

**D3. CETAS is the primary export, instead of JDBC reads from Spark or Lakehouse Federation.**
* *Why:* CETAS writes in parallel from every distribution of the dedicated pool, which is
  the fastest bulk path. Casting types inside the CETAS `SELECT` makes the Parquet
  predictable.
* *Rejected:* federation or JDBC reads funnel data through a few connections, and add
  load and runtime on large tables.

**D3a. The fallback is JDBC, not Copy Activity (added in 1.3).**
* When CETAS itself fails, the table is re-exported once by the later serverless
  fallback task through the Unity Catalog JDBC connection, using the same casting
  projection. The source is read once and staged as Parquet (`bridge/fallback.py`,
  `loader.uc_jdbc_export`). There is no in-task JDBC route.
* It is optionally a parallel read, when the mapping sets `jdbc_partition_column` (an
  integer or date column).
* *Why JDBC:* the owner's JDBC connection already works, so no new infrastructure is
  needed.
* *Rejected (Copy Activity):* it would need a pipeline deployment, calls to the
  Synapse management API, and polling. Its own Parquet encoding (dates as timestamps)
  would force alternate contract types; v6's Copy fallback was a major bug source.
* Both JDBC and Copy pull rows through the pool's control node, so neither is fast
  enough to be the primary route.

**D4. Cast types in the export query.** This fixes v6's unmappable types (`smallint`,
`tinyint`, `real`, `money`, datetimes) and removes the need for a Copy Activity fallback.

**D5. One workflow per night, with a for-each over batches, instead of one job run per table.**
* *Why:* v6 would have launched 2,000+ job runs a night, each paying start-up cost.
* The for-each concurrency equals the maximum number of concurrent CETAS statements,
  because tables inside a batch run sequentially.

**D6. Split deterministic and transient failures.** A deterministic failure (schema drift,
contract, empty source, target mismatch) cannot succeed on retry, so retrying only
wastes the cutoff window. This was bug #1 in the v6 review.

**D7. Retention is configuration.** An ADLS lifecycle rule handles staging files, and
`delta.deletedFileRetentionDuration` plus VACUUM or predictive optimization handles old
versions. v6's retention manifest grew without limit (bug #2).

**D8. The source count runs before CETAS** (optional per table via `verify_source_count`).
It is a second scan of the source, accepted as the price of detecting lost or changed
rows. Turn it off per table only with human approval.

**D9. Exact business-date gate on automatic runs (tightened in 1.5).** See invariant 11.

**D10. Row counts come from the upstream manifest (added in 1.4).**
* *Why:* `COUNT_BIG(*)` is a full extra scan of every table every night, and the
  upstream already knows its row counts.
* *Rejected:* judging freshness by the file's last-modified date (UTC timestamps, and
  rewrites on reruns — see invariant 12).
* *Rejected:* skipping the count check entirely. It is the only guard against a short
  export.

**D11. All source schemas are checked with one catalog query in the plan (added in 1.4).**
* This replaces 1,000 per-table `sys.columns` queries.
* It is safe because any schema change after the plan still fails validation: the
  Parquet column names, order and types are checked against the contract for every
  table.

**Net Synapse access per night (manifest mode, no failures):**
* two tiny catalog lookups (staging objects);
* one catalog query (all schemas);
* one CETAS per table.

No table is read except by its own export.

## 5a. Special characters and data errors

**Line breaks, carriage returns, pipes, quotes, tabs, NUL bytes and any Unicode
cannot break the export.** CETAS writes Parquet, which stores every string
length-prefixed; there are no row or column delimiters to collide with. This is proven
by `test_line_breaks_pipes_quotes_and_unicode_round_trip_exactly`.

Only a **delimited-text** file format would be affected. That is why the plan step
refuses any file format that is not PARQUET. Never "fix" a special-character problem by:

* switching to delimited text;
* adding `REPLACE()` calls to the export;
* escaping characters.

Doing any of those changes the data.

**Things that *can* fail CETAS, and how the package handles them:**

* **`datetime2(7)`/`datetimeoffset(7)` near the maximum value.** Rounding to microseconds
  overflows, for example on `9999-12-31 23:59:59.9999999`. Prevented: the seventh digit is
  truncated first (`contract._to_micros`).
* **Bad data in the source or view.** Examples: an overflowing CAST in a view, a string
  that can't convert to a date, divide by zero. Recognized by error number
  (`synapse.DATA_ERROR_CODES`) or, when the serverless driver keeps only the SQLSTATE, by
  SQLSTATE class `22` (data exception). Classified as `FAILED_SOURCE_DATA` (final
  for the night, table keeps old data). Fix the data or the view upstream; do not loosen
  the export.
* **Capacity or connection errors** (tempdb, timeouts, throttling, failover). Classified
  as `FAILED_EXPORT` (retried once), with no fallback. To make another *proven*
  data-error code final, add it to `synapse.DATA_ERROR_CODES` with a test.
* **Anything else raised by CETAS** (storage write failures, PolyBase/CETAS limits,
  external-table errors). The table is marked `FALLBACK_REQUIRED`; the later serverless UC-JDBC task performs the fallback read and logs `VALIDATED` on success.
  * If both routes fail, the status is `FAILED_EXPORT`.
  * If the fallback hits a data error, the status is `FAILED_SOURCE_DATA`.
* **Invalid columns/objects (207/208) or permission denied (229/230)** do not
  invoke JDBC. They end as `FAILED_SCHEMA_DRIFT` or `FAILED_SOURCE_ACCESS`
  respectively, pending a reviewed source or permission fix.
* **Other permission-denied codes** (likely 262, 297, 300, 15247; listed in
  `synapse.ACCESS_ERROR_CANDIDATES`) are not built in, because none has been
  observed from this pool yet. Until confirmed they fall back to JDBC, which only
  needs SELECT and can hide a lost CETAS grant behind routine fallbacks. When the
  trial shows one, add it to `execution.additional_access_error_codes`; it then ends
  as `FAILED_SOURCE_ACCESS` without using a fallback slot (added in 2.5).
* **`(n)varchar(max)`, binary, `xml`, spatial types.** Refused by the contract at
  generation time; they need a reviewed view.

## 6. Status vocabulary

| Status | Kind | Meaning |
|---|---|---|
| `PUBLISHED` | success | This release is committed with the right row count |
| `ALREADY_CURRENT` | success | The target still has this verified release; nothing exported |
| `SUPERSEDED` | success | A newer night is already committed; Delta skipped ours |
| `FAILED_SCHEMA_DRIFT` | deterministic | Source columns differ from the inventory hash |
| `FAILED_SOURCE_ACCESS` | deterministic | Synapse rejected source/export permissions; review the job identity |
| `FAILED_CONTRACT` | deterministic | Parquet names, order or types wrong, or a conversion would lose values |
| `FAILED_TARGET_SCHEMA` | deterministic | The target is missing or differs from the inventory |
| `REJECTED_EMPTY` | deterministic | 0 rows and `allow_empty` is false |
| `FAILED_SOURCE_DATA` | deterministic | CETAS hit a data error in the source, e.g. a conversion or overflow (codes in `synapse.DATA_ERROR_CODES`) |
| `FAILED_VERIFY_ROLLED_BACK` | deterministic | The committed count was wrong; restored automatically |
| `FAILED_VERIFY` | deterministic | The committed count was wrong; not restored because newer commits exist |
| `FAILED_UPSTREAM_SEQUENCE` | deterministic | A newer Delta commit's date/sequence contradicts the requested release |
| `FAILED_EXPORT` | transient | CETAS or connection error |
| `FAILED_COUNT_MISMATCH` | transient | Parquet row count differs from the source count |
| `FAILED_WRITE` | transient | No commit found for this release |
| `FAILED_UNKNOWN` | transient | An unexpected exception |
| `SKIPPED_CUTOFF` | skipped | Not started or not submitted for publication before the cutoff |
| `STARTED` | in flight | Written at the start of each attempt |
| `FALLBACK_STARTED` | in flight | JDBC route has started |
| `FALLBACK_REQUIRED` | in flight | CETAS could not serve the table; serverless UC-JDBC fallback must stage and validate it |
| `VALIDATED` | in flight | Immutable staged data passed row and schema checks; a write retry may revalidate it |
| `ROLLED_BACK` | manual | Written by the rollback notebook |

## 7. How to make common changes safely

**Add a source type mapping**
1. Add a branch in `contract.column_contract` (export expression, Parquet type, target
   type).
2. Add a row to `test_type_mapping`, and a local-Spark test if the Parquet type is new.
3. Confirm the real CETAS encoding on a trial night: watch for `FAILED_CONTRACT`.
4. If the pool can encode it two ways, add both to `contract.PARQUET_ACCEPT`.

**Add or change a table**
1. Regenerate or edit its inventory entry and bump `inventory_version`.
2. Render the setup SQL: new tables are created; a changed table needs a
   human-reviewed `ALTER TABLE`.
3. Redeploy, then run the nightly job with `only_mappings=<id>`.

**Add a control-log field**
1. Update the DDL string in `control.py` **and** `CONTROL_TABLES` in
   `tools/render_uc_setup.py`. `test_control_log_schemas_match_render` enforces this.
2. Once installed, existing UC tables need a reviewed `ALTER TABLE ADD COLUMN`,
   applied in each environment before deploying the code that writes it.

**Adapt to the real manifest CSV**
1. Put the actual header names in `manifest.columns`, for example
   `{"schema": "SchemaName", "row_count": "RowCount"}`. Set `manifest.delimiter` if it
   is not a comma.
2. `upstream_sequence` is optional. Without it the sequence is `yyyymmdd * 100`. A
   same-day correction must supply `yyyymmdd01`, `…02`, and so on, or it will be
   skipped as `ALREADY_CURRENT`.
3. The manifest must name the object that is actually exported. If a mapping exports a
   filtering **view**, the count must be the view's.
4. Run `tests/test_manifest.py` with a sample of the real file before deploying.

**Make the fallback fast for a large table**
1. Set `jdbc_partition_column` on the mapping to an integer or date column with a
   reasonably even spread.
2. `execution.jdbc_num_partitions` controls the parallel reads (default 4). In 4A the
   read goes through the UC JDBC connection, which must allow the `partitionColumn`,
   `lowerBound`, `upperBound`, `numPartitions`, `fetchsize`, `queryTimeout` and
   `preferTimestampNTZ` options (the acceptance job proves it). Each one
   uses a pool concurrency slot, so keep it modest.
3. Budget the worst case as `export_concurrency * jdbc_num_partitions` connections;
   the default is 4 * 4 = 16. The fallback count limit is per batch, not per night.

**Watch the fallback**
* `SELECT mapping_id, error_code, detail FROM <ctl>.table_log WHERE status = 'FALLBACK_REQUIRED'`.
  In 4A the extraction phase logs the cause on `FALLBACK_REQUIRED`: a non-null `error_code` is a
  CETAS error (`0` = SQL Server returned only a SQLSTATE, named in `detail`); NULL is a historical
  timestamp reroute. Only CETAS errors count toward the finalize alert. A failed UC-JDBC attempt is
  logged `FAILED_EXPORT`/`FAILED_COUNT_MISMATCH` with `method = 'JDBC'` and is re-attempted by the
  task retry or a Repair run.

**Tune throughput**
1. Change `export_concurrency` in `databricks.yml` (serverless has no cluster size to tune), and
   `max_tables_per_batch`/`max_bytes_per_batch` in the inventory.
2. Keep `export_concurrency` within what the pool's workload group allows.

**Serverless boundary in 4A.** The nightly path is fully serverless on the Databricks side. CETAS DDL is submitted from serverless with Microsoft `mssql-python`; the governed UC JDBC connection is reserved for bulk fallback reads. CETAS itself still executes inside the Synapse dedicated SQL pool.

## 8. Things an agent must not do without explicit human approval

* Add a policy to `governance.exempt_abac_policies`. Each entry is a recorded human
  decision that the policy's `EXCEPT` clause names both the job and the rollback
  identity (see `contract.governance_problems`). An entry identifies one policy by
  its defining securable and name (`{catalog, schema?, table?, name}`); never widen
  it to a bare name. Table-level filters and masks can never be exempted.
* Raise `execution.max_failed_table_ratio` or treat an error as transient in
  `synapse.is_transient_platform_error` without evidence from a real failure.
* Run the nightly job against production, run `rollback.py` with `dry_run=false`, or run
  `maintenance.py` with `vacuum=true` on production.
* Change `txnAppId` or `txnVersion` semantics, the step order in `process_table`, or the
  datetime mapping.
* Create, alter or drop target tables, or grant or revoke permissions.
* Create new connections, credentials, identities, external locations or Synapse external
  objects (the environment is already connected; see section 2).
* Turn off `verify_source_count`, raise retention to large values, or loosen validation
  to make a failing table pass.
* Replace CETAS with federation, JDBC reads or Copy Activity as the primary route; make
  the JDBC fallback run for `DATA` or `CAPACITY` errors; or add codes to
  `CAPACITY_ERROR_CODES`/`DATA_ERROR_CODES`/`ACCESS_ERROR_CODES` or
  `execution.additional_access_error_codes` without evidence from a real failure.
* Make an automatic RESTORE act on a commit from another job run.
* Remove or loosen the exact business-date gate on automatic runs.
* Hard-code `pause_status` in `databricks.yml`, remove the prod approval or
  `main`-only rule from the deploy workflow, let a job parameter choose the
  inventory file, or make the workflow run Synapse/Unity Catalog DDL or grants.
* Judge manifest freshness by file timestamps, let a manifest count skip validation,
  or remove the one-time Synapse verification on a manifest mismatch.

## 9. Verification status

| Claim | Status |
|---|---|
| Contract, validation, SQL builders, identifier safety | Unit-tested |
| Batching, cutoff (including Sydney DST), history decisions | Unit-tested |
| Per-table flow, every failure path, lost acknowledgement, retry scope | Tested with fake platform operations |
| Parquet validation, ANSI-safe loss detection, wall-clock preservation, `INSERT OVERWRITE` SQL | Tested on a real local Spark session |
| JVM JDBC command connection, error codes from real Java exceptions, the fallback's Parquet staging (`loader.stage_jdbc_query`, unpartitioned and partitioned) including newlines, pipes, NULLs and `9999-12-31` | Tested over real JDBC against Derby (`tests/test_jdbc_derby.py`); the Unity Catalog connection options are proven by `acceptance_checks.py` |
| Fallback routing: which errors fall back, which are final, which are retried | Unit-tested with fake operations |
| Manifest parsing and trust rules (BOM, CRLF, quoting, custom headers and delimiter, mixed nights, bad or duplicate rows, sequence revisions) | Unit-tested (`tests/test_manifest.py`) |
| Reading the manifest's exact bytes from a file | Tested on local Spark (the `text` source was found to split lines and drop the BOM; `binaryFile` is used) |
| Per-table count paths: manifest count, missing count, zero rows, wrong manifest confirmed or refused | Unit-tested with fake operations |
| Rechecking an existing commit: evidence order, no automatic RESTORE of another job run's commit, own-run lost-ack restore | Unit-tested with fake operations (`tests/test_v25_regressions.py`) |
| Manifest problems of any kind never block `synapse_table` mode | Unit-tested (`manifest.optional_counts`) |
| Deadline judged by Delta commit time; history renders it in UTC | Unit-tested, projection on local Spark; the real `DESCRIBE HISTORY` column is checked by `acceptance_checks.py` |
| Spark-free test modules collect and pass without PySpark | Tested (`test_suite_collects_without_pyspark`) |
| One-query schema hashes equal the generator's per-object hashes | Unit-tested |
| `databricks.yml` structure | YAML-parsed and compute placement unit-tested locally; run `databricks bundle validate -t dev` against the real DEV workspace before deployment |
| Rendered UC DDL | Parsed by the Spark SQL parser |
| **Delta skips equal/lower `txnVersion` on SQL `INSERT OVERWRITE`; history metadata; RESTORE** | **Unverified until `acceptance_checks.py` passes on the workspace** |
| The CETAS Parquet encoding for each cast type | Unverified until the first trial night |
| `DATEADD`/`DATEPART` with `nanosecond` accepted by the dedicated pool (scale-7 truncation) | Unverified until the first trial night; a table with a `datetime2(7)` column exercises it |
| That the existing connection has the CETAS permissions and a token path (section 2) | Unverified; check before the first run |
| Grants surviving the overwrite on real targets | Unverified until the first trial night |
| Governed-target writes and RESTORE on serverless publish compute | Unverified until `acceptance_checks.py` passes on the real DEV workspace |
| Real SQL Server/Synapse error codes for CETAS-specific failures reaching the fallback | Unverified; review `FALLBACK_REQUIRED` rows with a non-null `error_code` during the trial and first weeks |
| The real upstream CSV's layout, and that the upstream writes the success marker last | Unverified; confirm with the owner and a sample file (section 10) |

## 10. Questions to ask the human when unsure

* How does the Synapse command connection authenticate (service credential, service principal, or SQL login), and does that identity have the CETAS permissions?
* What are the names of the existing Synapse external data source and file format, and
  the Databricks external location for the staging container?
* Which tables are mandatory, which may legitimately be empty, and what is the real
  rollback window needed?
* What are the upstream CSV's exact header names, delimiter and file path? Does it
  include the business date and run id? Is a `_SUCCESS` marker written **after** the
  CSV is complete (or the CSV written under a temporary name and renamed)?
* Does each manifest count describe exactly the object that is exported (the view, if
  a mapping exports a filtering view)?
