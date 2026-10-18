# Setup checklist — service principal (plain language)

A tick-through guide from nothing to a nightly job running by itself. It assumes
**one service principal** both runs the Databricks jobs and logs in to Synapse
(`synapse.auth.mode = "client_secret"`). Details and edge cases are in
`docs/RUNBOOK.md`.

Do Steps 1–15 for **dev** first. Then repeat Steps 3–14 for **prod** (Step 15).
Step 16 is go-live; Part D is the daily routine.

The package never creates identities, secrets, connections, storage locations
or Synapse objects by itself. Every step that creates or grants something below
is done **by a person**, with the tool or script named in that step.

---

## Script map: what runs, who runs it, how often

| Script / file | What it does | Who runs it, where | How often |
|---|---|---|---|
| `sql/synapse/03_review_credential_scope.sql` | Read-only: lists the pool's credentials and data sources for a security review | Synapse admin, Synapse Studio / SSMS | Once per pool; again when credentials are added |
| `sql/synapse/01_export_setup.sql` | Creates the Synapse export objects (if missing), the job user and its grants | Synapse admin, Synapse Studio / SSMS | Once per pool |
| `infra/adls_lifecycle_policy.json` | Deletes staging files after 3 days | Azure admin, `az` CLI | Once per staging storage account |
| `config/settings.json` (from `settings.example.json`) | Your environment's settings | You, text editor | Once per environment |
| `tools/generate_inventory.py` | Reads the Synapse catalog and writes `config/inventory.<env>.json` | You, laptop with access to Synapse | Once per environment; again when tables change |
| `tools/render_uc_setup.py` | Writes `build/uc_setup.sql` (UC tables, views, grants) | You, laptop | Once per environment; again when tables change |
| `build/uc_setup.sql` | Creates control and target tables, grants the service principal | Databricks admin, SQL warehouse | Once per environment; again when tables change |
| `tools/prepare_deploy.py` | Checks the inventory and copies it to `config/inventory.json` | You (manual deploy) or the GitHub workflow | Every deploy |
| `databricks bundle deploy` | Creates or updates the four Databricks jobs | You or the GitHub workflow | Every deploy |
| `.github/workflows/ci-cd.yml`, `bundle-deploy.yml` | Tests on pull requests; deploy dev then prod | GitHub, automatically | Every merge to `main` |
| Job *synapse-full-refresh-acceptance* (`acceptance_checks.py`) | Proves Delta, serverless, JDBC and CETAS behaviour on the workspace | You, Databricks Jobs UI | Once per environment; after platform updates |
| Job *synapse-full-refresh-serverless-cetas-serverless-jdbc* (`00_plan` → `10` → `20` → `30` → `90_finalize`) | The nightly copy | Databricks schedule, 01:30 Sydney | **Every day, automatically** |
| Job *synapse-full-refresh-maintenance* (`maintenance.py`) | VACUUM, log pruning, leftover cleanup | Databricks schedule, Sunday 14:00 Sydney | **Weekly, automatically** |
| Job *synapse-full-refresh-rollback* (`rollback.py`) | Restores one table to an earlier version | Rollback approvers, by hand | Only when bad data was published |
| `tests/` (`python -m pytest tests -q`) | Local checks | You / GitHub | After any code or config change |

The notebooks `00_plan.py`, `10_…`, `20_…`, `30_…`, `90_finalize.py` and
`_bootstrap.py` are never run by hand; they are the tasks of the nightly job.

---

## Part A — Collect these names first (nothing is changed)

| Done | # | What | Where to find it | Example |
|---|---|---|---|---|
| [ ] | A1 | Synapse server and dedicated pool | Azure portal | `myws.sql.azuresynapse.net`, `mypool` |
| [ ] | A2 | The **service principal**: display name, application (client) ID, tenant ID | Entra ID → App registrations (reuse the one your JDBC code uses) | `sp-bridge`, `1234-abcd-…`, `9876-…` |
| [ ] | A3 | Name of the Databricks secret scope for the principal's tenant ID, client ID and client secret, and the key names (created in Step 2b if it does not exist) | `databricks secrets list-scopes` | `bridge-secrets`: `sp-tenant-id`, `sp-client-id`, `sp-client-secret` |
| [ ] | A4 | Unity Catalog JDBC connection to the pool | `SHOW CONNECTIONS` | `synapse_jdbc` |
| [ ] | A5 | Staging container and storage account (CETAS writes Parquet here) | Azure portal / `SHOW EXTERNAL LOCATIONS` | `staging` in `stgacct` |
| [ ] | A6 | Existing UC external location covering the staging container | `SHOW EXTERNAL LOCATIONS` | `stage_loc` |
| [ ] | A7 | Upstream container and folder for the daily CSV and marker (a **different** container) | Agree with the upstream person | `upstream` in `ctlacct`, folder `upstream-manifests` |
| [ ] | A8 | UC external location covering the upstream container | `SHOW EXTERNAL LOCATIONS` | `upstream_loc` |
| [ ] | A9 | Existing Synapse external data source / Parquet file format for staging, if any | `SELECT name, location FROM sys.external_data_sources; SELECT name, format_type FROM sys.external_file_formats;` | none → Step 4 creates `bridge_stage`, `bridge_parquet` |
| [ ] | A10 | Synapse source schema(s) to copy | Synapse | `dbo` |
| [ ] | A11 | Target catalog and schema for the copied tables | Your decision | `analytics.gold` |
| [ ] | A12 | Control catalog and schema for the job's logs | Your decision | `analytics.bridge_control` |
| [ ] | A13 | Scratch schema for the acceptance test | Your decision | `analytics.bridge_scratch` |
| [ ] | A14 | Groups: operators (rerun), rollback approvers (a different group), consumers (read), a test group | Databricks admin | `ops`, `rollback-approvers`, `analysts`, `bridge-testers` |
| [ ] | A15 | Failure email address | You | `ops@company.com` |
| [ ] | A16 | Webhook URL for the nightly summary email (e.g. an Azure Logic App) and the secret key that will hold it | You / Azure admin | scope `bridge-secrets`, key `nightly-summary-webhook-url` |
| [ ] | A17 | Dev and prod workspace URLs | Databricks | `https://adb-111.azuredatabricks.net` |

---

## Part B — One-time setup, in order (dev first)

### Step 1 — Agree the daily files with the upstream person

- [ ] Send them `docs/UPSTREAM_MANIFEST_SPEC.md`, `config/row_counts.example.csv`
      and `config/success_marker.example.json`.
- [ ] Agree:
  * one folder per day: `upstream-manifests/<yyyy-mm-dd>/`;
  * `row_counts.csv` with header `business_date,upstream_run_id,schema,table,row_count`,
    one row per table, `upstream_run_id` = the business date;
  * the marker file (e.g. `_success_marker.json`) written **last**, in the same folder, containing
    `{"business_date": "2026-10-12", "upstream_run_id": "2026-10-12", "upstream_sequence": 2026101200}`;
  * `business_date` = the Sydney date of the morning the bridge loads;
  * both files present by **01:30 Sydney**; files are never moved or rewritten.

### Step 2 — Make sure the service principal exists in both places

Nothing in the package creates it (AGENTS.md section 2). An admin checks, and
creates only with approval:

- [ ] **Entra ID**: the app registration exists (A2) and has a client secret.
- [ ] **Databricks**: Account console → Service principals → the same application ID
      is added and assigned to the workspace.
- [ ] **Deploying person or identity** has the *Service Principal User* role on
      this service principal (needed because the jobs run as it).

### Step 2b — Put the secrets in a Databricks secret scope

The job reads four values from one secret scope at run time. Only the scope
name and key names go into `config/settings.json`; the values never appear in
any file. Creating a scope is a one-time admin action; agree it with your
Databricks admin first (AGENTS.md section 2).

| Key (your choice of name) | Value | Where to get it |
|---|---|---|
| `sp-tenant-id` | Entra tenant ID | Entra ID → Overview |
| `sp-client-id` | The service principal's application (client) ID | Entra ID → App registrations → the app |
| `sp-client-secret` | A client secret **value** of that app | App → Certificates & secrets → New client secret (copy the *Value*, shown only once) |
| `nightly-summary-webhook-url` | Full HTTPS URL of the summary webhook (A16) | The Logic App / workflow trigger |

Run these from a terminal with the Databricks CLI logged in to the workspace
(`databricks auth login --host <A17 dev URL>`; add `--profile <name>` to each
command if you use profiles).

- [ ] **1. Check whether a scope already exists:**

  ```bash
  databricks secrets list-scopes
  ```

  If a suitable one exists, skip to 3 and use its name.

- [ ] **2. Create the scope** (Databricks-backed):

  ```bash
  databricks secrets create-scope bridge-secrets
  ```

  Your organisation may require an *Azure Key Vault-backed* scope instead. Then
  an admin creates it in the workspace UI at
  `https://<workspace-url>#secrets/createScope` (DNS name and resource ID of the
  Key Vault), and the four values are added as **Key Vault secrets** with the key
  names above, not with the `put-secret` commands in step 3.

- [ ] **3. Add the four secrets.** Run each command without a value: the CLI
      prompts for it, so it does not end up in your shell history.

  ```bash
  databricks secrets put-secret bridge-secrets sp-tenant-id
  databricks secrets put-secret bridge-secrets sp-client-id
  databricks secrets put-secret bridge-secrets sp-client-secret
  databricks secrets put-secret bridge-secrets nightly-summary-webhook-url
  ```

  Running a command again overwrites the value, e.g. when the client secret is
  rotated.

- [ ] **4. Let the service principal read the scope:**

  ```bash
  databricks secrets put-acl bridge-secrets <A2 application-id> READ
  ```

- [ ] **5. Check** (lists key names only, never values):

  ```bash
  databricks secrets list-secrets bridge-secrets
  databricks secrets list-acls bridge-secrets
  ```

  Expect the four keys, and `READ` for the application ID. You (the creator)
  have `MANAGE`; nobody else should need access.

- [ ] **6. Use the names** in Step 7: `synapse.auth.secret_scope`,
      `tenant_id_key`, `client_id_key`, `client_secret_key`, and
      `notifications.summary_webhook.secret_scope`, `url_secret_key`.

Client secrets expire (Entra shows the expiry date). Put a reminder in your
calendar and rerun `put-secret … sp-client-secret` with the new value before then,
or the night fails at login.

### Step 3 — Synapse: security review (read-only)

- [ ] A Synapse admin runs `sql/synapse/03_review_credential_scope.sql` in the pool.
- [ ] The data owner approves what it lists. `ALTER ANY EXTERNAL DATA SOURCE`
      (granted in Step 4) reaches **every** database-scoped credential in the pool.
      If that is unacceptable, stop and redesign (runbook, step 1).

### Step 4 — Synapse: run `01_export_setup.sql` (required)

This creates whatever is missing (`IF NOT EXISTS`) and grants the service
principal what CETAS needs. Run it once per pool.

- [ ] Open `sql/synapse/01_export_setup.sql` and find-and-replace **every** occurrence of:

  | Placeholder | Replace with |
  |---|---|
  | `<staging-container>` | A5 container, e.g. `staging` |
  | `<storage-account>` | A5 account, e.g. `stgacct` |
  | `<databricks-job-identity>` | A2 **display name**, e.g. `sp-bridge` |
  | `<source-schema>` | A10, e.g. `dbo` (copy the `GRANT SELECT` line for each extra schema) |

- [ ] If A9 found an existing data source or Parquet format you want to reuse,
      either change `bridge_stage` / `bridge_parquet` in the script to those names
      or delete those two sections. Use the same names in Step 7.
- [ ] Run it as a pool administrator. It creates: schema `bridge_meta`, the master
      key, credential `bridge_msi`, data source `bridge_stage`, file format
      `bridge_parquet`, the user for the service principal, and these grants:
      `SELECT` on the source schema; `CREATE TABLE`; `ALTER` on `bridge_meta`;
      `ALTER ANY EXTERNAL DATA SOURCE`; `ALTER ANY EXTERNAL FILE FORMAT`;
      `ADMINISTER DATABASE BULK OPERATIONS`.
- [ ] Check: `SELECT name, location FROM sys.external_data_sources;` shows
      `bridge_stage` at `abfss://<staging-container>@<storage-account>.dfs.core.windows.net`.
- [ ] Optional: the commented workload-group section at the end of the script
      limits how much of the pool the export can use.

### Step 5 — Azure storage

- [ ] **Synapse workspace managed identity** has *Storage Blob Data Contributor* on
      the staging container (CETAS writes with it). Azure portal → container →
      Access control (IAM).
- [ ] **Lifecycle rule**: in `infra/adls_lifecycle_policy.json` replace `<container>`
      with A5's container, then:

  ```bash
  az storage account management-policy create \
      --account-name <A5 account> --resource-group <resource group> \
      --policy @infra/adls_lifecycle_policy.json
  ```

### Step 6 — Unity Catalog prerequisites (manual grants)

`build/uc_setup.sql` (Step 10) only *mentions* these as comments, so a
Databricks admin runs them in a SQL editor:

- [ ] The UC JDBC connection (A4) exists and its own credential has `SELECT` on
      the source schema. The package never creates it.
- [ ] Staging location: read **and** write (the fallback writes there):

  ```sql
  GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `<A6>` TO `<A2 application-id>`;
  ```

- [ ] Upstream location: read **only**. Check the principal has no write access
      there, including inherited storage RBAC:

  ```sql
  GRANT READ FILES ON EXTERNAL LOCATION `<A8>` TO `<A2 application-id>`;
  ```

- [ ] Acceptance scratch schema (A13):

  ```sql
  CREATE SCHEMA IF NOT EXISTS <catalog>.<scratch_schema>;
  GRANT USE CATALOG ON CATALOG <catalog> TO `<A2 application-id>`;
  GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.<scratch_schema> TO `<A2 application-id>`;
  ```

### Step 7 — Fill in `config/settings.json`

- [ ] Copy `config/settings.example.json` to `config/settings.json`. Each setting is
      explained in its `_comments` block. Never put passwords here.

| Done | Setting | Value |
|---|---|---|
| [ ] | `inventory_version` | e.g. `wave_001` |
| [ ] | `source_set_id` | keep `synapse_nightly`; never change it later |
| [ ] | `synapse.server`, `synapse.database` | A1 |
| [ ] | `synapse.auth` | see below |
| [ ] | `synapse.uc_jdbc_connection` | A4 |
| [ ] | `upstream.completion_source` | keep `manifest` |
| [ ] | `manifest.path_template` | `abfss://<A7 container>@<A7 account>.dfs.core.windows.net/upstream-manifests/{business_date}/row_counts.csv` |
| [ ] | `manifest.success_marker` | the agreed marker name, e.g. `_success_marker.json` |
| [ ] | `manifest.columns`, `manifest.delimiter` | keep as they are |
| [ ] | `staging.root` | `abfss://<A5 container>@<A5 account>.dfs.core.windows.net` |
| [ ] | `staging.prefix` | keep `bridge-stage` (must match the lifecycle rule) |
| [ ] | `staging.metadata_schema`, `external_data_source`, `file_format` | the names from Step 4 (default `bridge_meta`, `bridge_stage`, `bridge_parquet`) |
| [ ] | `control.catalog`, `control.schema` | A12 |
| [ ] | `notifications.summary_webhook.secret_scope`, `url_secret_key` | A16 |
| [ ] | `execution.*` | keep the defaults |

`synapse.auth` for a service principal:

```json
"auth": {
  "mode": "client_secret",
  "secret_scope": "bridge-secrets",
  "tenant_id_key": "sp-tenant-id",
  "client_id_key": "sp-client-id",
  "client_secret_key": "sp-client-secret"
}
```

### Step 8 — Fill in `databricks.yml`

- [ ] Replace `https://<dev-workspace>.azuredatabricks.net` and
      `https://<prod-workspace>.azuredatabricks.net` with A17.

### Step 9 — Generate the inventory

On a laptop that can reach Synapse, with the *Microsoft ODBC Driver 18 for SQL
Server* installed. You sign in interactively as yourself; you need read access
to the Synapse catalog.

- [ ] Install and run:

  ```bash
  pip install -r requirements-dev.txt
  export BRIDGE_SYNAPSE_ODBC='Driver={ODBC Driver 18 for SQL Server};Server=tcp:<A1 server>,1433;Database=<A1 pool>;Encrypt=yes;Authentication=ActiveDirectoryInteractive;'
  python tools/generate_inventory.py --settings config/settings.json \
      --source-schema <A10> --target-catalog <A11 catalog> --target-schema <A11 schema> \
      --output config/inventory.dev.json --profile
  ```

  Repeat `--source-schema` for several schemas. Add `--exclude-file exclude.txt`
  (one `schema.object` per line) to skip objects.
- [ ] If it prints "No inventory written": the listed columns have unsupported
      types (`varchar(max)`, binary, `xml`, spatial). Exclude those objects or copy
      them through a reviewed view, then rerun.
- [ ] Open `config/inventory.dev.json` and set per table:
  * `mandatory`: `true` if the night must fail when this table fails;
  * `allow_empty`: `true` if zero rows is legitimate;
  * `estimated_bytes` for heavy **views** (the tool prints which views have 0).
- [ ] The tool refuses to overwrite an existing file; delete or rename it to regenerate.

### Step 10 — Create the Databricks tables and grants

- [ ] Render the SQL:

  ```bash
  python tools/render_uc_setup.py --inventory config/inventory.dev.json \
      --output build/uc_setup.sql --job-principal <A2 application-id> --consumer-group <A14 consumers>
  ```

- [ ] A Databricks admin reviews `build/uc_setup.sql` and runs it in a SQL
      warehouse. It creates the control schema and tables (`run_log`, `table_log`,
      `plan`), the views `table_latest` and `table_current`, the target schemas and
      every target table, and grants the service principal `USE CATALOG`,
      `USE CONNECTION`, `USE SCHEMA` and `SELECT, MODIFY` on each target. It is safe
      to rerun.
- [ ] If a target table **already existed**, it keeps its old properties. Review and run
      `ALTER TABLE <target> SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days')`.

### Step 11 — Run the tests locally

- [ ] `python -m pytest tests -q` passes.

### Step 12 — Deploy the jobs

Either **by hand**:

- [ ] Log in once: `databricks auth login --host <A17 dev URL>`.
- [ ] Deploy:

  ```bash
  python tools/prepare_deploy.py --environment dev
  databricks bundle validate -t dev
  databricks bundle deploy -t dev --var job_identity=<A2 application-id> \
      --var notification_email=<A15> --var operations_group=<A14 operators> \
      --var rollback_approvers_group=<A14 approvers>
  ```

Or **through GitHub** (once: see the runbook, "One-time GitHub setup"):

- [ ] GitHub Environments `dev` and `prod` (prod: required reviewers, `main` only).
- [ ] Federated credential for the deploying identity per environment.
- [ ] Environment variables: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
      `DATABRICKS_JOB_IDENTITY` (A2 application ID), `NOTIFICATION_EMAIL`,
      `OPERATIONS_GROUP`, `ROLLBACK_APPROVERS_GROUP`. Leave the `*_PAUSE_STATUS`
      variables unset.
- [ ] Commit `config/settings.json`, `config/inventory.dev.json` and `databricks.yml`,
      open a pull request, merge to `main`. The workflow tests, runs
      `prepare_deploy.py`, and deploys dev. It then waits for prod approval:
      **do not approve yet.**

Result: four jobs in the workspace (nightly, acceptance, rollback, maintenance),
all paused. In dev, job names start with `[dev …]`.

### Step 13 — Acceptance test

- [ ] Databricks → Jobs → *synapse-full-refresh-acceptance* → Run with parameters:
      `scratch_schema=<A13>` and `grant_principal=<A14 test group>`.
- [ ] **Do not continue until the output ends with `ALL ACCEPTANCE CHECKS PASSED`.**

### Step 14 — Trial run

Full walk-through with every command and check: `docs/FIRST_RUN.md`. In short:

- [ ] Put today's CSV and marker in `upstream-manifests/<today>/` (real ones from
      the upstream person, or hand-made for a test).
- [ ] Run the nightly job with `business_date=<today>` and
      `only_mappings=<2–3 mapping_ids from the inventory>`.
      Outside 01:30–07:00 a run must give `business_date`, and works only until
      11:00 Sydney. For daytime dev tests, set
      `execution.recovery_cutoff_local_time` to e.g. `"20:00"` in
      `config/inventory.dev.json` only, and redeploy.
- [ ] Check the results:

  ```sql
  SELECT mapping_id, status, method, detail FROM <A12>.table_latest ORDER BY event_utc DESC;
  ```

  Expect `PUBLISHED`. Watch for `FAILED_CONTRACT` (type encoding differs).
- [ ] Run again for all tables; check the summary email arrived.
- [ ] Complete the "Environment checks" in `docs/RELEASE_VALIDATION.md`.

### Step 15 — Prod

- [ ] Repeat Steps 3–14 for prod: prod pool and storage, a prod
      `config/settings.json` (keep a separate copy), `--output config/inventory.prod.json`,
      `render_uc_setup.py --inventory config/inventory.prod.json`, deploy with
      `--environment prod` / `-t prod` (or approve the waiting prod deployment in GitHub).

### Step 16 — Go live

- [ ] Set the schedules to run: by hand,
      `--var nightly_pause_status=UNPAUSED --var maintenance_pause_status=UNPAUSED`
      on the prod deploy; with GitHub, set `NIGHTLY_PAUSE_STATUS=UNPAUSED` and
      `MAINTENANCE_PAUSE_STATUS=UNPAUSED` on the `prod` Environment and run the
      workflow for `prod`.
- [ ] Do **not** unpause only in the Databricks UI; the next deploy pauses it again.
- [ ] Unless predictive optimization is on, set the maintenance job's `vacuum`
      parameter to `true`.

---

## Part D — Every day

**Upstream person (automatic, every night)**

1. Rebuild the Synapse tables.
2. Write `upstream-manifests/<date>/row_counts.csv`.
3. Write the marker in the same folder **last**, by **01:30 Sydney**.

**Databricks (automatic, 01:30 Sydney; nobody starts it)**

`00_plan` waits up to 60 minutes for both files → `10_extract_validate_batch`
exports each table with CETAS and validates it → `20_serverless_jdbc_fallback`
re-reads only tables CETAS could not export → `30_publish_batch` replaces each
target in one step and verifies it → `90_finalize` records the night and sends
the summary email.

**You (each morning, about 5 minutes)**

1. Read the summary email.
2. If something failed:

   ```sql
   SELECT event, business_date, detail FROM <A12>.run_log ORDER BY event_utc DESC LIMIT 5;
   ```

   and use "Common situations" in `docs/RUNBOOK.md`:
   * temporary error before 07:00 → **Repair run** on the job;
   * temporary error 07:00–11:00 → run the nightly job with
     `business_date=<today>` and `only_mappings=<failed ids>`;
   * after 11:00 → leave it; those tables keep yesterday's data until the next night.

**Weekly (automatic, Sunday 14:00 Sydney):** the maintenance job tidies logs and
leftovers.

**Only when something changes**

* **Table added or columns changed**: regenerate (or edit) `config/inventory.<env>.json`,
  bump `inventory_version`, re-render and run `build/uc_setup.sql` (new tables) or a
  reviewed `ALTER TABLE` (changed tables), redeploy, run with `only_mappings=<id>`.
* **Bad data published**: stop other writers to that table, run the **rollback** job
  with `dry_run=true`, then with `dry_run=false` and `expected_current_version`
  (approvers group only).
* **New credentials added to the Synapse pool**: rerun Step 3.
