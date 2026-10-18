# Runbook

Throughout, `<ctl>` means `<control catalog>.<control schema>` from the inventory.

## Quick start: the whole installation in order

For one person doing every part except the upstream manifest (which the upstream
team writes to storage, see `docs/UPSTREAM_MANIFEST_SPEC.md`). Each phase points
to the detailed step; do them in this order, first for dev, then for prod.

**Phase 0: collect these names before changing anything**

| What | Where to find it | Goes into |
|---|---|---|
| Synapse server and dedicated pool | Azure portal | `synapse.server`, `synapse.database` |
| How you log in to Synapse today (SQL login, service principal or managed identity) | Your existing JDBC code; AGENTS.md section 2 | `synapse.auth` |
| Databricks job service principal (application ID) | Entra ID / Databricks admin | `--job-principal`, `job_identity` |
| Synapse external data source and Parquet file format for the staging container | `SELECT name, location FROM sys.external_data_sources` | `staging.external_data_source`, `staging.file_format` |
| Staging container and its Databricks external location | `SHOW EXTERNAL LOCATIONS` | `staging.root` |
| Unity Catalog JDBC connection to Synapse (required for the fallback) | `SHOW CONNECTIONS` | `synapse.uc_jdbc_connection` |
| Manifest path agreed with the upstream team | `docs/UPSTREAM_MANIFEST_SPEC.md` section 1 | `manifest.path_template` |
| Control catalog, target catalog and schema, consumer group | Your design | settings, `--target-*`, `--consumer-group` |
| Secret scope for the summary webhook URL | Databricks workspace | `notifications.summary_webhook` |

**Phases**

| Phase | Do | Detailed step |
|---|---|---|
| 1. Synapse | Review credential scope, create only missing objects and grants | One-time setup, step 1 |
| 2. Storage | ADLS lifecycle rule; manifest container read-only for the job | Step 2, `infra/README.md` |
| 3. Unity Catalog prerequisites | External location grants, UC JDBC connection, webhook secret | Steps 3 and 5 |
| 4. Configure | `settings.json` → generate and review the inventory → render and run `uc_setup.sql` | Steps 3, 4, 5 |
| 5. Deploy to dev | By hand or through GitHub | Step 6, "Deploying with GitHub Actions" |
| 6. Prove in dev | Acceptance job, then trial runs | Steps 7 and 8, `docs/RELEASE_VALIDATION.md` |
| 7. Prod and go-live | Same for prod, then unpause through the bundle variables | "Fresh installation" steps 12–14, step 9 |
| Daily | Summary email and morning queries | "Every morning", "Common situations" |

## Fresh installation with GitHub (dev, then prod)

Use this checklist for a new installation where nothing is deployed yet. It
follows the release path **GitHub → dev, verify, then GitHub → prod**. Each step
points to the detailed section below; the details are not repeated here.
A new installation does **not** run any changelog upgrade action: the rendered
`uc_setup.sql` creates every control table with its current columns.

**A. Prepare dev and prod (each environment, before its first deploy)**

1. Synapse: review credential scope, create only missing objects
   (One-time setup, step 1).
2. Upstream completion contract and ADLS lifecycle policy (steps 2–3).
3. Settings for that environment, and the summary-webhook URL in a
   Databricks secret (step 3 and `config/settings.example.json`).
4. Generate, review and save `config/inventory.dev.json` /
   `config/inventory.prod.json` (step 4).
5. Render and run `uc_setup.sql` for that environment (step 5).

**B. GitHub, once**

6. Real workspace hosts in `databricks.yml`; commit both inventories
   (One-time GitHub setup, steps 1–2).
7. GitHub Environments `dev` and `prod` (prod: required reviewers, `main`
   only), federated credentials and variables. Leave the pause variables
   unset (One-time GitHub setup, steps 3–5).
8. Service Principal User role for the deploying identity on the job's
   service principal (One-time GitHub setup, step 4).

**C. Deploy to dev and verify**

9. Pull request → tests pass → merge to `main`. The workflow deploys **dev**
   and then waits at the prod approval. Do **not** approve yet.
10. In dev: run the acceptance job (step 7); run the nightly job manually with
    `only_mappings=<a few ids>`, then all tables (step 8); check `table_log`
    and the summary email. Dev schedules stay paused (About the dev target).
11. If anything is wrong: **reject** the pending prod deployment, fix it in a
    new pull request, and repeat from step 9.

**D. Deploy to prod**

12. When dev is verified, approve the prod deployment **of the same workflow
    run** you verified in dev, so prod receives exactly the tested commit. If
    you merged again in between, verify the newer run in dev first.
    (Alternative: *Run workflow* → `prod` from `main`.)
13. In prod: run the acceptance job, a small `only_mappings` load, and the
    "Environment checks" in `docs/RELEASE_VALIDATION.md`. The schedule
    is still paused.
14. **Go live**: set `NIGHTLY_PAUSE_STATUS=UNPAUSED` and
    `MAINTENANCE_PAUSE_STATUS=UNPAUSED` on the `prod` Environment, then *Run
    workflow* → `prod` to redeploy (step 9). Confirm the schedule is active.

**Every later change** follows the same path: pull request → merge → dev
deploy → verify in dev → approve prod. Apply any required Unity Catalog or
Synapse change to each environment before its deploy.

## One-time setup (in order)

1. **Synapse** — *this environment is already connected to Databricks: reuse it.*
   * Run `sql/synapse/03_review_credential_scope.sql` as a pool administrator.
     `ALTER ANY EXTERNAL DATA SOURCE`, required by this CETAS route, reaches
     every database-scoped credential in the database. Record the owner-approved
     credential scope. Isolate the export in a pool without unrelated
     credentials or redesign its export trigger if that reach is unacceptable;
     a separate database cannot simply query the dedicated pool's source
     tables. New credentials added later
     require a new review; a schema-level grant does not narrow this permission.
   * Confirm the existing job identity has the CETAS permissions listed in
     `sql/synapse/01_export_setup.sql`. Grant only what is missing, with approval.
   * Find the existing external data source and file format that write to the staging
     container, and put their names in the settings.
   * Run `01_export_setup.sql` only for objects that do **not** exist yet. It is
     `IF NOT EXISTS`-guarded, but review the placeholders first.
   * The upstream signals completion with the row-count CSV and its `_SUCCESS`
     marker (step 3); no Synapse completion table is created. Agree that file
     contract with the upstream team now: send them `docs/UPSTREAM_MANIFEST_SPEC.md`
     with `config/row_counts.example.csv` and `config/success_marker.example.json`.
   * See AGENTS.md section 2 for the four capabilities to check, and why a Lakehouse
     Federation connection alone is not enough.
2. **Storage**: apply `infra/adls_lifecycle_policy.json` (see `infra/README.md`).
3. **Settings**: copy `config/settings.example.json` to `config/settings.json` and fill it in.
   * Provision an HTTPS workflow endpoint (for example an Azure Logic App) to
     deliver the nightly summary email. Store its complete URL in a Databricks
     secret and set `notifications.summary_webhook.secret_scope` and
     `url_secret_key`. Grant the job identity READ on this secret. The endpoint
     must accept a JSON POST, send one message to operators, and deduplicate on
     `notification_id` (also sent as the `Idempotency-Key` header). Do not put
     the URL in the inventory, repository, or logs. Test network access from
     serverless compute before enabling the nightly schedule.
   * `synapse.auth`: reuse the identity of your existing JDBC connection (table in AGENTS.md
     section 2). SQL login → `sql_password`; service principal → `client_secret`; managed
     identity → `service_credential`.
     For `client_secret`, the plan uses `azure-identity`, which the job environment in
     `databricks.yml` installs together with `mssql-python`. The acceptance job fails
     if the environment is not applied to the notebook tasks. The other two
     authentication modes do not use `azure-identity`.
   * `execution.upstream_wait_minutes`: how long the plan waits for tonight's upstream.
   * `execution.plan_transient_retry_minutes` (default 15): the plan has no task
     retries, so it retries transient errors itself (pool capacity or unavailable
     codes, connection/timeout SQLSTATEs, Java network errors) for this long, never
     past the cutoff. A malformed manifest, wrong staging objects or any other
     error still fails the plan at once.
   * `execution.max_failed_table_ratio` (default 0.02): finalize fails the job when
     more than this share of planned tables did not refresh, whatever the
     `mandatory` flags and `fail_on_any_failure` say.
   * `governance.exempt_abac_policies` (default empty): see *Governed targets* below.
   * Automatic runs require today's Sydney business date; `max_upstream_lag_days` is deprecated and has no effect.
   * Optionally set `jdbc_partition_column` on large mappings, so the fallback reads in
     parallel.
   * `manifest`: the path template of the upstream CSV (with `{business_date}` or
     `{business_date_compact}`), the success marker, and the real header names and
     delimiter. `upstream.completion_source` must be `manifest`: the CSV and its marker are the
     "upstream finished" signal (`tools/prepare_deploy.py` refuses anything else).
     Place CSV and marker in a separate container where the job has read access
     only. Do not grant the job storage-account-wide contributor access.
   * In manifest completion mode, `_SUCCESS` must contain JSON with the exact
     `business_date`, `upstream_run_id`, and integer `upstream_sequence` of the CSV.
     Remove the previous marker **before** a same-day upstream correction begins,
     write the CSV atomically, then write the new marker last. An empty marker no
     longer certifies completion. Test this contract with the upstream team.
   * The upstream must wait for `FINISHED` or `FINISHED_WITH_FAILURES` and
     confirmed absence of active exports (or explicit operator release of a
     failed job) before a same-day correction, even after removing `_SUCCESS`.
     A Synapse dedicated pool defaults to READ UNCOMMITTED; verify the pool's
     isolation setting. RCSI alone cannot protect a multi-table export from
     later committed writes. This scheduler fence is required in both modes.
   * Size JDBC reads to the pool's workload slots: concurrent fallback reads
     can reach `export_concurrency * jdbc_num_partitions` (default 4 * 4 = 16).
     The CETAS-error fallback limit is per batch and resets on a retry.
     Pre-1900 timestamp reroutes do not consume that budget but still reread
     their tables; measure their number and time on the trial night. Keep the
     conservative 1900 threshold until real CETAS and JDBC boundary-value
     comparisons establish a safe alternative. Include DATE columns in that trial.
     The fallback alert counts only CETAS-error fallbacks. Finalize reports
     historical timestamp reroutes separately; investigate their volume and
     runtime even when the job succeeds.
4. **Inventory**:
   ```bash
   pip install -r requirements-dev.txt
   export BRIDGE_SYNAPSE_ODBC='Driver={ODBC Driver 18 for SQL Server};Server=tcp:<ws>.sql.azuresynapse.net,1433;Database=<pool>;Encrypt=yes;Authentication=ActiveDirectoryInteractive;'
   python tools/generate_inventory.py --settings config/settings.json \
       --source-schema dbo --target-catalog <catalog> --target-schema <schema> \
       --output config/inventory.json --profile
   ```
   * **Deploying with GitHub Actions:** generate one reviewed inventory per
     environment and commit it as `config/inventory.dev.json` and
     `config/inventory.prod.json` (use `--output` with that name, and
     `--settings` pointing at that environment's settings). The workflow copies
     the right one to `config/inventory.json` at deploy time.
   * If objects are reported as unsupported, either cast or exclude their columns in a
     reviewed view, or list them in `--exclude-file`.
   * Then review `mandatory` and `allow_empty` for each table.
5. **Unity Catalog**:
   * Reuse the existing external location on the staging container, and grant the job
     identity READ FILES **and WRITE FILES** on it (the fallback writes there).
   * A Unity Catalog JDBC connection to the Synapse pool must exist (`SHOW CONNECTIONS`);
     put its name in `synapse.uc_jdbc_connection`. The package never creates it. Its
     identity needs SELECT on the source schemas, and the connection must allow the
     `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, `fetchsize`,
     `queryTimeout` and `preferTimestampNTZ` options (the acceptance job proves it). The
     rendered setup SQL grants the job identity `USE CONNECTION` on it.
   * Grant the job identity READ FILES on the manifest's location.
   * Verify the job has no WRITE FILES, storage RBAC write, or inherited write
     permission on the separate upstream manifest container. Restrict READ FILES
     on the staging location to the job and approved incident responders: staged
     Parquet is unmasked source data. Successful exports are deleted after publication;
     failed and interrupted exports remain until lifecycle cleanup.
   * A target table that already exists keeps its own properties: the setup SQL uses
     `CREATE TABLE IF NOT EXISTS`. Review and apply `ALTER TABLE ... SET TBLPROPERTIES
     ('delta.deletedFileRetentionDuration' = 'interval 7 days')` on such a table to
     get the seven-day rollback window.
   * Render and run the setup SQL in a SQL warehouse:
     ```bash
     python tools/render_uc_setup.py --inventory config/inventory.json \
         --output build/uc_setup.sql --job-principal <sp-application-id> --consumer-group <group>
     ```
   * Review `SHOW GRANTS ON TABLE <target>` and inherited schema/catalog rights
     for every target. Only the bridge writer should have routine MODIFY rights;
     also grant the bridge identity sufficient metadata visibility to inspect
     `SHOW EFFECTIVE POLICIES` and `DESCRIBE TABLE EXTENDED ... AS JSON`.
     The worker fails closed before exporting a governed table
     or when it cannot inspect those controls. Keep administrator and emergency
     access in an approved change window.
     Automated verification and RESTORE assume no second writer can commit
     between the version check and the restore.
6. **Deploy** (with GitHub Actions, see "Deploying with GitHub Actions" below;
   by hand from a workstation, as follows). First validate and select that
   environment's inventory; without this step `config/inventory.json` does not exist:
   ```bash
   python tools/prepare_deploy.py --environment dev      # or prod
   databricks bundle deploy -t dev --var job_identity=<sp-application-id> \
       --var notification_email=<ops@company> \
       --var operations_group=<operations-group> \
       --var rollback_approvers_group=<separate-approvers-group>
   ```
   Confirm effective job ACLs, bundle-file ACLs, service-principal run identity,
   and inherited admin rights. The inventory is fixed to the deployed bundle path;
   the nightly group can override business_date/only_mappings but cannot select
   another inventory file. Rollback access is limited to its separate approvers.
7. **Acceptance**: run the *synapse-full-refresh-acceptance* job with
   `scratch_schema=<catalog>.<scratch_schema>` and
   `grant_principal=<existing-test-group>`. **Do not continue until it prints
   ALL ACCEPTANCE CHECKS PASSED.**
8. **Trial night** in non-production:
   * Test data: the manifest CSV and its `_SUCCESS` marker for **today's Sydney date**
     must be in the manifest folder before the run (`docs/UPSTREAM_MANIFEST_SPEC.md`).
   * Test window: a run without parameters works only before the 07:00 publication
     cutoff, and a run with `business_date=<today>` only until `recovery_cutoff_local_time`
     (11:00). To test during the day in dev, set `recovery_cutoff_local_time` later
     (for example `"20:00"`) in `config/inventory.dev.json` only; keep prod at 11:00.
   * Run the nightly job with `business_date=<today>` and `only_mappings=<a few ids>`,
     then with all tables.
   * Check `table_log` for `FAILED_CONTRACT`. Those rows show where the pool's Parquet
     encoding differs from what the contract expects.
   * Check that target grants remain intact and that governance preflight rejects
     any target with row filters, column masks, or effective ABAC filters/masks.
   * Verify a `datetime2` column through the serverless UC-JDBC fallback preserves
     wall-clock values (`preferTimestampNTZ`, session time zone UTC).
   * Force one table through the fallback and confirm it goes `FALLBACK_REQUIRED` →
     `VALIDATED` → `PUBLISHED`, including a partitioned read when
     `jdbc_partition_column` is set.
   * Export sentinel `date '0001-01-01'` and a timestamp before 1900 via actual
     CETAS, confirm the date is unchanged and that the historical timestamp takes
     the JDBC route with the same value as Synapse. A mismatched value blocks go-live.
9. **Go live**: unpause the nightly schedule, and the weekly maintenance job (set
   `vacuum=true` unless predictive optimization is on). Do this through the
   bundle variables `nightly_pause_status` and `maintenance_pause_status`
   (with GitHub: the `prod` Environment variables `NIGHTLY_PAUSE_STATUS` and
   `MAINTENANCE_PAUSE_STATUS` set to `UNPAUSED`, then redeploy prod). Do **not**
   unpause in the Databricks UI only: the next deploy would reset the schedule. The maintenance job
   also OPTIMIZEs the three control tables after pruning retained rows.

## Deploying with GitHub Actions

The package ships two workflows in `.github/workflows/`:

* `ci-cd.yml`: runs the tests on every pull request to `main`. On merge to
  `main` it deploys to **dev**, then to **prod** after approval. A manual run
  (*Run workflow*) deploys one chosen environment; prod only from `main`.
  Documentation-only changes do not trigger a deploy.
* `bundle-deploy.yml`: reusable deploy of one environment. It checks the
  GitHub Environment variables, runs `tools/prepare_deploy.py` (checks the
  target host is real, validates `config/inventory.<env>.json` and copies it to
  `config/inventory.json`), signs in to Azure with GitHub OIDC, then runs
  `databricks bundle validate` and `databricks bundle deploy -t <env>`.

### One-time GitHub setup

1. In `databricks.yml`, replace the `<dev-workspace>` and `<prod-workspace>`
   hosts with the real workspace URLs. The deploy fails while they are placeholders.
2. Commit `config/inventory.dev.json` and `config/inventory.prod.json` (setup step 4).
3. Create GitHub Environments named exactly `dev` and `prod`. On `prod`, add
   **required reviewers** and restrict deployment branches to `main`.
4. Deploying identity: reuse the Entra app your existing workflows use, or a
   dedicated one. Add a federated credential for each environment
   (subject `repo:<org>/<repo>:environment:dev` and `...:environment:prod`).
   In each workspace it needs permission to create and update the bundle's
   jobs and files. Because the jobs run as `job_identity`, the deploying
   identity must also have the **Service Principal User** role on that service
   principal (not needed if it is the same principal).
5. Add these **Environment variables** (not secrets; none of them is sensitive)
   to both `dev` and `prod`:

   | Variable | Value |
   |---|---|
   | `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` | Deploying identity and tenant. `AZURE_SUBSCRIPTION_ID` is optional. |
   | `DATABRICKS_JOB_IDENTITY` | Application ID of the service principal the jobs run as. |
   | `NOTIFICATION_EMAIL` | Databricks job failure email. |
   | `OPERATIONS_GROUP` | Group that may rerun the nightly job. |
   | `ROLLBACK_APPROVERS_GROUP` | Separate group that may run rollback. |
   | `EXPORT_CONCURRENCY` | Optional, default 4. |
   | `NIGHTLY_PAUSE_STATUS`, `MAINTENANCE_PAUSE_STATUS` | Optional, default `PAUSED`. Set `UNPAUSED` on `prod` only at go-live. |

   If your existing workflows sign in to Databricks differently (for example
   Databricks-native GitHub OIDC rather than `azure/login`), change the login
   step and `DATABRICKS_AUTH_TYPE` in `bundle-deploy.yml` to match them, and
   pin `databricks/setup-cli` to the CLI version your other workflows use.

### What the workflow does not do

These stay manual, per environment, and must be done **before** the deploy
that needs them:

* Synapse SQL (`sql/synapse/`), the ADLS lifecycle policy, and the Databricks
  secret holding the summary webhook URL.
* Unity Catalog setup (`build/uc_setup.sql`) for new tables, and any reviewed
  `ALTER TABLE` for a changed control or target table. Apply it before merging
  code that writes new columns, or the next nightly run fails.
* Running the acceptance job and the trial night.

### About the dev target

`dev` uses bundle development mode. The Databricks CLI then prefixes job names
with `[dev <deploying identity>]`, deploys under that identity's home folder,
and always keeps schedules paused, whatever the pause variables say. Run dev
jobs manually. If you want dev to behave like prod (shared path, real
schedule), change its `mode` in `databricks.yml` after review.

### Changing a table through GitHub

Update `config/inventory.<env>.json` (bump `inventory_version`), open a pull
request, run any UC DDL for that environment first (see "Adding or changing a
table"), then merge. Dev deploys automatically; prod deploys after approval.

## Every morning

```sql
-- Result of the latest run
SELECT event, business_date, detail FROM <ctl>.run_log ORDER BY event_utc DESC LIMIT 5;

-- Inspect the most recent run's summary; a fresh rerun is scoped to its job_run_id.
SELECT job_run_id, detail FROM <ctl>.run_log
WHERE event IN ('FINISHED','FINISHED_WITH_FAILURES')
ORDER BY event_utc DESC LIMIT 1;

-- Tables that needed the JDBC fallback tonight (slower; fix CETAS if a table repeats).
-- error_code not null = CETAS error (0 = only a SQLSTATE, see detail); null = timestamp reroute.
SELECT mapping_id, error_code, detail FROM <ctl>.table_log
WHERE status = 'FALLBACK_REQUIRED' AND event_utc > current_timestamp() - INTERVAL 1 DAY;

-- Where row counts came from in the latest job run
WITH events AS (
  SELECT mapping_id, count_source,
         row_number() OVER (PARTITION BY mapping_id ORDER BY event_utc DESC) AS rn
  FROM <ctl>.table_log
  WHERE job_run_id = (SELECT job_run_id FROM <ctl>.run_log
                      WHERE event = 'PLANNED' ORDER BY event_utc DESC LIMIT 1)
)
SELECT count_source, count(*) FROM events WHERE rn = 1 GROUP BY count_source;

-- Manifest warnings and notes for the latest night
SELECT get_json_object(detail, '$.manifest'), get_json_object(detail, '$.manifest_note'),
       get_json_object(detail, '$.manifest_warnings')
FROM <ctl>.run_log WHERE event = 'PLANNED' ORDER BY event_utc DESC LIMIT 1;

-- Slowest tables (capacity planning)
SELECT mapping_id, method, duration_seconds FROM <ctl>.table_latest
WHERE status = 'PUBLISHED' ORDER BY duration_seconds DESC LIMIT 20;

-- What each target is serving now
SELECT mapping_id, release_id, delta_version, event_utc FROM <ctl>.table_current ORDER BY mapping_id;
```

The job fails and sends failure notifications if more than `max_failed_table_ratio`
(default 2%) of planned tables did not refresh, if any mandatory table did not refresh, or if
any table's Delta commit landed after the 07:00 Sydney deadline (judged by the commit
time in `table_log.commit_utc`, not by when the log row or finalize ran; the late tables
are listed in the `FINISHED_WITH_FAILURES` summary as `late_tables`). `fail_on_any_failure` defaults to `false`;
set it to `true` if optional failures must also fail and alert the job.

After all batches finish, finalize sends **one structured run summary** to the
configured HTTPS endpoint, even if a small number of optional tables failed and
the job itself succeeds. The payload contains `total_tables`, `successful_tables`,
`failed_tables`, all failed mapping IDs and statuses in `failures`, and the
same-day recovery cutoff. Configure the endpoint to format and send the email.
If delivery fails, finalize fails so the existing job failure email alerts
operators; the run summary remains in `run_log`. The job's existing on-failure
email can also fire when the run fails for a table threshold or another reason;
that system alert is separate from the structured summary. A manual Repair run
may redeliver a summary; the endpoint must deduplicate by `notification_id`.
An early plan failure has no table outcomes and uses the existing job failure
email instead.

## Line breaks, pipes, quotes in the data

These do not break anything: the export is Parquet, not delimited text, and every
character round-trips exactly.

If the plan fails with *"must be PARQUET"*, the reused Synapse file format is delimited
text. Point `staging.file_format` at a Parquet format; `01_export_setup.sql` creates
`bridge_parquet` if you need one.

## Common situations

| Status | Meaning | Action |
|---|---|---|
| `FAILED_EXPORT`, `FAILED_COUNT_MISMATCH`, `FAILED_WRITE`, `FAILED_UNKNOWN` | Transient; already retried once | Before 07:00 use **Repair run**. From 07:00 to 11:00 start a new run with today's `business_date` and `only_mappings` set to failed IDs. Reconfirm the upstream write fence. After 11:00 leave them for the next night. |
| `FAILED_SCHEMA_DRIFT` | The source columns changed | Regenerate that mapping and review it. Alter the target table deliberately (see below), redeploy, and rerun with `only_mappings=<id>`. |
| `FAILED_SOURCE_ACCESS` | Synapse denied source/export access | Review SELECT and CETAS permissions for the job identity, then rerun in a new job run. |
| `FAILED_CONTRACT` | The Parquet did not match the contract (type, order, or a lossy conversion) | Investigate the source or view; fix the mapping; rerun. |
| `FAILED_TARGET_SCHEMA` | The target table differs from the inventory | Someone changed it, or setup was not run. Fix the table, never the check. |
| `FAILED_SOURCE_DATA` | CETAS hit bad data in the source or view (overflow, failed conversion, divide by zero); `detail` has the SQL error number | Fix the data or the view upstream, then rerun with `only_mappings=<id>`. Do not loosen the export. |
| `REJECTED_EMPTY` | 0 rows and `allow_empty` is false | Confirm with the upstream team. Set `allow_empty` if empty is legitimate. |
| Plan fails: *"No COMPLETE upstream for expected business date"* | Tonight's manifest CSV and `_SUCCESS` marker had not appeared within `upstream_wait_minutes` (the detail says what was found) | Nothing changed. Once the upstream completes, start the job again, with no parameters or with `business_date=<yyyy-mm-dd>`. |
| `FALLBACK_REQUIRED` followed by `PUBLISHED`, with `method = JDBC` | JDBC delivered the data after a CETAS error (`error_code` set; `0` = SQLSTATE only) or a pre-1900 timestamp reroute (`error_code` null) | Repeated CETAS errors need investigation; measure timestamp reroute runtime separately. Consider `jdbc_partition_column` for large mappings. |
| `FAILED_EXPORT` or `FAILED_COUNT_MISMATCH` with `method = JDBC` | Both CETAS and the UC-JDBC fallback failed (the fallback read is bounded by the publication deadline) | Read `detail`; fix the cause, then Repair the run before the cutoff or wait for the next night. |
| Plan fails: *"no manifest found for …"* | The upstream CSV or its `_SUCCESS` marker was not there within `upstream_wait_minutes` | Nothing changed. Once the upstream finishes, run again. To publish a specific night, pass `business_date`. |
| Plan fails with a manifest error (missing column, mixed dates, bad sequence) | The CSV is malformed or for another night | Fix the CSV (or the `manifest.columns` mapping), then run again. |
| `count_source = synapse_after_manifest_mismatch` | The manifest count was wrong; Synapse confirmed the export, which was published | Tell the upstream team which table's count is wrong. |
| `SKIPPED_CUTOFF` | No publication started before the configured cutoff and safety margin | Start a new targeted same-day run before 11:00; otherwise leave it for the next night. |
| `FAILED_VERIFY_ROLLED_BACK` / `FAILED_VERIFY` | The committed row count was wrong, or (rerun) an earlier run's commit disagrees with the current evidence | Escalate. With `FAILED_VERIFY` (no automatic restore), inspect the target and use the rollback job if necessary. A corrected same-day publish requires a higher upstream sequence. |

Auto-compaction or predictive optimization may append an `OPTIMIZE` version
immediately after the bad write. The automatic restore then refuses to run,
because the bad version is no longer latest. Treat `FAILED_VERIFY` as needing
an operator to inspect history and use the rollback job under an exclusive
target-writer window. The acceptance job checks the actual `OPTIMIZE` name.

**Rerunning a night** means starting a new nightly job with today's Sydney
`business_date=<yyyy-mm-dd>` before 11:00 Sydney time. No historical date is
accepted by the nightly job. A repair of the original run still uses the
original 07:00 plan cutoff. Re-establish the upstream write fence before
any daytime extraction; a complete marker alone does not prevent a correction.
Tables already published report `ALREADY_CURRENT` without exporting. To recheck the
existing commit, the bridge first uses a prior verified publication of that exact live
Delta version and row metric, then the manifest count, and only then a bounded Synapse
count, which can continue after the publishing cutoff. If a commit made by an
**earlier job run** disagrees with that evidence, the table ends as `FAILED_VERIFY` and
is **not** restored automatically, because the source may have changed since that run:
inspect it and use the rollback job, or republish with a higher sequence. Only a bad
commit made by the current job run (a lost acknowledgement) is restored automatically. Prefer
`only_mappings=<id1,id2>` when repairing a few tables to avoid unnecessary
source scans and history queries. A corrected upstream dataset requires a
higher `upstream_sequence`; reusing the same release assumes immutable source data.

The default `publish_margin_minutes=5` applies to new exports and to publication:
a table does not start exporting, and a validated table is not published, when
fewer than five minutes remain, and export timeouts end at the margin. It reduces
late commits but cannot cap an in-flight Delta overwrite; finalize still reports
any commit after the deadline. The nightly job
does not support target-table row filters or column masks with this dedicated
`INSERT OVERWRITE` design. Review ABAC policies and RESTORE behavior separately
before using governed targets.

### Governed targets and inherited ABAC policies

`SHOW EFFECTIVE POLICIES` also lists row filter and column mask policies inherited
from the catalog or schema, whether or not their tag conditions match the table.
A single catalog-wide policy therefore blocks every target in that catalog as
`FAILED_TARGET_SCHEMA` (and the finalize failure-ratio alarm fires). The
acceptance job runs the same preflight on its scratch table, so this shows up
before go-live.

For each blocking ABAC policy, run `DESCRIBE POLICY` and check its `EXCEPT` clause.
Only when it names **both** the job identity and the rollback identity may the
policy be listed in `governance.exempt_abac_policies`, with the review recorded
in the inventory change. Each entry names the securable the policy is defined on,
as shown in the `Catalog`, `Schema` and `Table` columns of `SHOW EFFECTIVE POLICIES`:

```json
"governance": {"exempt_abac_policies": [
  {"catalog": "analytics", "name": "pii_mask"},
  {"catalog": "analytics", "schema": "gold", "name": "region_filter"}
]}
```

Policy names are unique only per securable, so an exemption covers exactly that
one policy; a same-named policy on another catalog, schema or table still blocks.
A bare policy name is rejected when the inventory is loaded. Exempted principals are not subject to the policy, which
is what makes dedicated `INSERT OVERWRITE` and `RESTORE` possible for them.
Table-level row filters and column masks have no `EXCEPT` clause and can never be
exempted. Re-review the list whenever a policy's principals change.

### Runtime 18 updates

The nightly and acceptance jobs run on serverless environment version 3, which
Databricks updates in place; the maintenance and rollback job clusters use
`18.x-scala2.13` and pick up dated Runtime 18 updates when they start. Rerun the
acceptance job after any serverless or Runtime 18 release-note entry that lists
behavioural changes. Plan and finalize record the runtime build in
their `run_log` detail (`runtime`), so a regression can be matched to an update.

## Rollback a table

First stop overlapping writes to this target (including the nightly job and any
operator jobs), and confirm its current Delta version. A manual RESTORE is not
protected by the bridge's idempotent-write guard and can replace a newer commit
if another writer starts between inspection and restoration. Run the
*synapse-full-refresh-rollback* job with `mapping_id`, `to_business_date` (or
`to_version`) and `reason`. Keep `dry_run=true` first; it prints the observed
version. For the non-dry run, pass that version as `expected_current_version`
and set `dry_run=false`. The job refuses a changed current version, but only
the exclusive write window closes the gap between its final check and RESTORE.
The same exclusion is required for automatic restore after a bad commit. If a
second target writer is allowed to run concurrently with the nightly job, do
not enable the schedule: neither Delta RESTORE nor its preceding version check
is conditional on the target still being at that version.

Only versions inside `delta.deletedFileRetentionDuration` (7 days by default) can be
restored. The next normal night overwrites the table as usual. To republish the *same*
night after a rollback, the upstream must publish the manifest again with a higher
`upstream_sequence`.

## Adding or changing a table

1. Regenerate or edit the inventory entry. Bump `inventory_version`.
2. **New table**: re-render and run `uc_setup.sql` (`CREATE TABLE IF NOT EXISTS`).
   **Changed columns**: write a reviewed `ALTER TABLE` for the target first. The job
   never changes table schemas; it refuses to write when they differ.
3. Redeploy the bundle (with GitHub: merge the change to
   `config/inventory.<env>.json`; see "Deploying with GitHub Actions").
4. Run the nightly job with `only_mappings=<id>` to load the new table immediately.

## Maintenance (weekly job)

The weekly job:

* VACUUMs targets (when `vacuum=true`);
* prunes control logs older than `control_retention_days`;
* drops CETAS external-table definitions left by crashed exports only when their
  generated name matches an inventory mapping and a recorded `STARTED` attempt;
  unidentified definitions are left for manual investigation.

Staging files are removed by the ADLS lifecycle rule, not by this job.
