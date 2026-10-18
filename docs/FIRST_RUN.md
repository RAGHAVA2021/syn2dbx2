# First run — every step, in order

This guide takes you from a fresh copy of this package to a verified first load
of your tables, in order, with every command. It assumes one **service
principal** that both runs the Databricks jobs and logs in to Synapse.

Do it in **dev** first (Parts 1–5). Then repeat it for **prod** and go live
(Part 6).

| Part | What | Who |
|---|---|---|
| 1 | Write down the names you will need | You |
| 2 | Synapse and Azure storage | Synapse admin, Azure admin |
| 3 | Databricks: secrets, permissions, settings, tables, deploy | You, Databricks admin |
| 4 | First run with 2–3 tables, and checks | You |
| 5 | Load all tables | You |
| 6 | Prod and go-live | You |
| 7 | If something fails | You |

---

## Part 1 — Write down the names you will need

Nothing is changed in this part. Fill in the right-hand column; every command
below uses these placeholders.

| Placeholder | What it is | Where to find it | Example | Your value |
|---|---|---|---|---|
| `<synapse-server>` | Synapse SQL endpoint | Azure portal → Synapse workspace | `myws.sql.azuresynapse.net` | |
| `<synapse-pool>` | Dedicated SQL pool | Azure portal | `mypool` | |
| `<source-schema>` | Synapse schema with the tables to copy | Synapse | `dbo` | |
| `<sp-name>` | Service principal display name | Entra ID → App registrations | `sp-bridge` | |
| `<sp-app-id>` | Its application (client) ID | Same page, *Application (client) ID* | `1234abcd-…` | |
| `<tenant-id>` | Entra tenant ID | Entra ID → Overview | `9876fedc-…` | |
| `<staging-account>` | Storage account for exported Parquet | Azure portal | `stgacct` | |
| `<staging-container>` | Container in it | Azure portal | `staging` | |
| `<upstream-account>` | Storage account for the daily CSV and marker | Agree with the upstream person | `ctlacct` | |
| `<upstream-container>` | Container in it (**not** the staging container) | Same | `upstream` | |
| `<resource-group>` | Resource group of the staging account | Azure portal | `rg-data` | |
| `<uc-jdbc-connection>` | Unity Catalog JDBC connection to the pool | `SHOW CONNECTIONS` in Databricks | `synapse_jdbc` | |
| `<staging-location>` | UC external location over the staging container | `SHOW EXTERNAL LOCATIONS` | `stage_loc` | |
| `<upstream-location>` | UC external location over the upstream container | `SHOW EXTERNAL LOCATIONS` | `upstream_loc` | |
| `<target-catalog>`, `<target-schema>` | Where the copied tables go | Your decision | `analytics`, `gold` | |
| `<control-catalog>`, `<control-schema>` | Where the job's log tables go | Your decision | `analytics`, `bridge_control` | |
| `<scratch-schema>` | Schema for the acceptance test's scratch table, as `catalog.schema` | Your decision | `analytics.bridge_scratch` | |
| `<ops-group>` | Group allowed to rerun the nightly job (and view the maintenance job) | Databricks admin | `ops` | |
| `<approvers-group>` | Group allowed to run the rollback and acceptance jobs | Databricks admin | `ops` or `rollback-approvers` | |
| `<consumers-group>` | Group that reads the copied tables | Databricks admin | `analysts` | |
| `<test-group>` | Any existing group; the acceptance test grants it read access on a scratch table and then drops the table | Databricks admin | `ops` | |
| `<ops-email>` | Failure email address | You | `ops@company.com` | |
| `<webhook-url>` | HTTPS URL that sends the nightly summary email (e.g. an Azure Logic App trigger) | You / Azure admin | `https://prod-01…logic.azure.com/…` | |
| `<dev-workspace-url>`, `<prod-workspace-url>` | Databricks workspace URLs | Browser address bar | `https://adb-111.azuredatabricks.net` | |
| `<today>` | Today's **Sydney** date, `yyyy-mm-dd` | Calendar | `2026-10-15` | |

**One group for all three is allowed.** `<ops-group>`, `<approvers-group>` and
`<test-group>` may be the same group; nothing in the package requires them to
differ. Then use that one name wherever this guide shows any of the three.
The one consequence: rollback runs `RESTORE` on a target table (puts back an
older version of the data), so with one group everyone who can rerun the nightly
job can also roll back tables. That is fine in dev. For prod, decide it with the
data owner; a small separate approvers group is safer and is only a different
value for `rollback_approvers_group` when deploying.

Fixed names used in this guide (you can change them, but then change them everywhere):

| Name | What |
|---|---|
| `bridge-secrets` | Databricks secret scope |
| `bridge_meta`, `bridge_stage`, `bridge_parquet` | Synapse schema, external data source, file format |
| `bridge-stage` | Folder inside the staging container |
| `upstream-manifests/<date>/row_counts.csv` | The daily CSV |
| `upstream-manifests/<date>/_success_marker.json` | The daily marker, written last |

---

## Part 2 — Synapse and Azure storage

### Step 1 — Agree the daily files with the upstream person

The job starts copying only when both files for **today** exist. Agree with the
upstream person that every night, after all Synapse tables are rebuilt, they:

1. write `upstream-manifests/<date>/row_counts.csv` into `<upstream-container>`:

   ```
   business_date,upstream_run_id,schema,table,row_count
   2026-10-15,2026-10-15,dbo,Customer,1204331
   2026-10-15,2026-10-15,dbo,SalesOrder,58412907
   ```

   * one row per table, the count of rows in that table or view;
   * `business_date` and `upstream_run_id` both = the **Sydney date of the morning
     the job loads**, the same on every row (a pipeline finishing at 23:30 on
     14 October writes `2026-10-15`);
   * plain whole numbers, comma-separated, header in the first line;

2. then, **last**, write `upstream-manifests/<date>/_success_marker.json`, one line:

   ```json
   {"business_date": "2026-10-15", "upstream_run_id": "2026-10-15", "upstream_sequence": 2026101500}
   ```

   `upstream_sequence` is the date digits followed by `00`, written as a number;

3. finish both by **01:30 Sydney**, and never move, rename or rewrite the files.

- [ ] Agreed with the upstream person (send them `docs/UPSTREAM_MANIFEST_SPEC.md`).

### Step 2 — Synapse: security review (read-only)

A Synapse admin, in Synapse Studio or SSMS, connected to `<synapse-pool>`:

- [ ] Open `sql/synapse/03_review_credential_scope.sql` and run it. It changes nothing.
- [ ] Show the result to the data owner. The permission granted in Step 3
      (`ALTER ANY EXTERNAL DATA SOURCE`) can reach **every** credential it lists.
      Continue only with their approval.

### Step 3 — Synapse: run `01_export_setup.sql`

Required, once per pool. It creates the Synapse objects the export needs (only
those missing) and gives the service principal its permissions.

- [ ] Check what already exists:

  ```sql
  SELECT name, location FROM sys.external_data_sources;
  SELECT name, format_type FROM sys.external_file_formats;
  ```

  If a data source already points at
  `abfss://<staging-container>@<staging-account>.dfs.core.windows.net` and a
  PARQUET format exists, you may reuse them: replace `bridge_stage` /
  `bridge_parquet` in the script with their names, and use the same names in Step 8.
- [ ] Open `sql/synapse/01_export_setup.sql` and use find-and-replace for **every**
      occurrence of:

  | Replace | With |
  |---|---|
  | `<staging-container>` | your staging container |
  | `<storage-account>` | your staging account |
  | `<databricks-job-identity>` | `<sp-name>` (the display name) |
  | `<source-schema>` | your source schema |

  For more than one source schema, copy the `GRANT SELECT ON SCHEMA` line once per schema.
- [ ] Run the whole script as a pool administrator. It stops with *"Replace all
      placeholders"* if you missed one.
- [ ] Check:

  ```sql
  SELECT name, location FROM sys.external_data_sources WHERE name = 'bridge_stage';
  SELECT name FROM sys.database_principals WHERE name = '<sp-name>';
  ```

It creates: schema `bridge_meta`, the database master key, credential
`bridge_msi`, data source `bridge_stage`, file format `bridge_parquet`, the
database user `<sp-name>`, and grants it `SELECT` on the source schema,
`CREATE TABLE`, `ALTER` on `bridge_meta`, `ALTER ANY EXTERNAL DATA SOURCE`,
`ALTER ANY EXTERNAL FILE FORMAT` and `ADMINISTER DATABASE BULK OPERATIONS`.

### Step 4 — Azure storage

An Azure admin:

- [ ] Give the **Synapse workspace managed identity** the role *Storage Blob Data
      Contributor* on `<staging-container>` (Azure portal → storage account →
      container → Access control (IAM) → Add role assignment). CETAS writes with it.
- [ ] Add the cleanup rule that deletes staging files after 3 days. In
      `infra/adls_lifecycle_policy.json` replace `<container>` with your staging
      container, then:

  ```bash
  az login
  az storage account management-policy create \
      --account-name <staging-account> --resource-group <resource-group> \
      --policy @infra/adls_lifecycle_policy.json
  ```

---

## Part 3 — Databricks

### Step 5 — Install the tools on your laptop

- [ ] Python 3.12, and from the package folder:

  ```bash
  pip install -r requirements-dev.txt
  ```

- [ ] *Microsoft ODBC Driver 18 for SQL Server* (needed in Step 10).
- [ ] The Databricks CLI, then log in to the dev workspace:

  ```bash
  databricks auth login --host <dev-workspace-url>
  databricks current-user me
  ```

### Step 6 — The service principal in Databricks

A Databricks account admin:

- [ ] Account console → User management → Service principals → **Add** →
      Microsoft Entra ID managed → `<sp-app-id>` (skip if already there).
- [ ] Assign it to the dev workspace (Account console → Workspaces → the
      workspace → Permissions → Add).
- [ ] Give **yourself** (the person deploying) the *Service Principal User* role on
      it: Workspace settings → Identity and access → Service principals →
      `<sp-name>` → Permissions. The jobs run as this principal, so whoever
      deploys them needs that role.

### Step 7 — Put the secrets in a secret scope

The job reads four values at run time. Only their **names** go into the
settings file; the values never appear in any file.

| Key | Value | Where to get it |
|---|---|---|
| `sp-tenant-id` | `<tenant-id>` | Entra ID → Overview |
| `sp-client-id` | `<sp-app-id>` | Entra ID → App registrations → the app |
| `sp-client-secret` | A client secret **value** | The app → Certificates & secrets → New client secret → copy the *Value* (shown only once) |
| `nightly-summary-webhook-url` | `<webhook-url>` | Your Logic App / workflow trigger |

- [ ] See whether a scope exists already:

  ```bash
  databricks secrets list-scopes
  ```

- [ ] Create the scope (skip if you reuse one, and use its name below instead):

  ```bash
  databricks secrets create-scope bridge-secrets
  ```

  If your organisation requires an *Azure Key Vault-backed* scope, an admin
  creates it at `<dev-workspace-url>#secrets/createScope`, and the four values are
  added as Key Vault secrets with the key names above instead of the next commands.
- [ ] Add the four values. Each command prompts for the value, so it is not saved
      in your shell history:

  ```bash
  databricks secrets put-secret bridge-secrets sp-tenant-id
  databricks secrets put-secret bridge-secrets sp-client-id
  databricks secrets put-secret bridge-secrets sp-client-secret
  databricks secrets put-secret bridge-secrets nightly-summary-webhook-url
  ```

- [ ] Let the service principal read them:

  ```bash
  databricks secrets put-acl bridge-secrets <sp-app-id> READ
  ```

- [ ] Check (shows key names only):

  ```bash
  databricks secrets list-secrets bridge-secrets
  databricks secrets list-acls bridge-secrets
  ```

Client secrets expire. Note the expiry date shown in Entra and rerun the
`sp-client-secret` command with a new value before then.

### Step 8 — Fill in `config/settings.json`

- [ ] Copy the template:

  ```bash
  cp config/settings.example.json config/settings.json
  ```

- [ ] Edit `config/settings.json`. Each setting is explained in its `_comments`
      block at the top. Change these:

  | Setting | Value |
  |---|---|
  | `synapse.server` | `<synapse-server>` |
  | `synapse.database` | `<synapse-pool>` |
  | `synapse.auth` | the block below |
  | `synapse.uc_jdbc_connection` | `<uc-jdbc-connection>` |
  | `manifest.path_template` | `abfss://<upstream-container>@<upstream-account>.dfs.core.windows.net/upstream-manifests/{business_date}/row_counts.csv` |
  | `manifest.success_marker` | `_success_marker.json` |
  | `staging.root` | `abfss://<staging-container>@<staging-account>.dfs.core.windows.net` |
  | `staging.external_data_source`, `staging.file_format` | `bridge_stage`, `bridge_parquet` (or the names you reused in Step 3) |
  | `control.catalog`, `control.schema` | `<control-catalog>`, `<control-schema>` |
  | `notifications.summary_webhook.secret_scope` | `bridge-secrets` |

  ```json
  "auth": {
    "mode": "client_secret",
    "secret_scope": "bridge-secrets",
    "tenant_id_key": "sp-tenant-id",
    "client_id_key": "sp-client-id",
    "client_secret_key": "sp-client-secret"
  }
  ```

  Leave everything else as it is. In particular never change `source_set_id`
  later.

### Step 9 — Fill in `databricks.yml`

- [ ] Replace `https://<dev-workspace>.azuredatabricks.net` with `<dev-workspace-url>`
      and `https://<prod-workspace>.azuredatabricks.net` with `<prod-workspace-url>`.

### Step 10 — Generate the table list (inventory)

This reads every table and view in the source schema from Synapse and writes
`config/inventory.dev.json`. You sign in to Synapse as yourself in a browser
window; you need read access to the Synapse catalog.

- [ ] Run:

  ```bash
  export BRIDGE_SYNAPSE_ODBC='Driver={ODBC Driver 18 for SQL Server};Server=tcp:<synapse-server>,1433;Database=<synapse-pool>;Encrypt=yes;Authentication=ActiveDirectoryInteractive;'
  python tools/generate_inventory.py --settings config/settings.json \
      --source-schema <source-schema> \
      --target-catalog <target-catalog> --target-schema <target-schema> \
      --output config/inventory.dev.json --profile
  ```

  * More schemas: repeat `--source-schema`.
  * Skip objects: add `--exclude-file exclude.txt` with one `schema.object` per line.
  * It never overwrites an existing file; delete the old one to regenerate.
- [ ] If it prints *"No inventory written"*: the listed columns have types that
      cannot be copied directly (`varchar(max)`, binary, `xml`, spatial). Exclude
      those objects, or copy them through a view, and run it again.
- [ ] Open `config/inventory.dev.json` and, for each table:
  * `mandatory`: `true` if the night must fail when this table fails;
  * `allow_empty`: `true` if zero rows is normal for it;
  * for heavy **views**, set `estimated_bytes` (the tool lists views with 0).
- [ ] **For the first run only**, allow manual runs until 20:00 so you can test
      during the day. In `config/inventory.dev.json`, under `execution`, set:

  ```json
  "recovery_cutoff_local_time": "20:00"
  ```

  Dev only. Prod keeps `11:00`.

### Step 11 — Permissions in Unity Catalog

A Databricks admin, in a SQL editor:

- [ ] Check the JDBC connection exists, and that the credential stored in it has
      `SELECT` on the source schema in Synapse:

  ```sql
  SHOW CONNECTIONS;
  ```

- [ ] Staging: read **and** write:

  ```sql
  GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `<staging-location>` TO `<sp-app-id>`;
  ```

- [ ] Upstream: read **only**. Also check in Azure that the principal has no write
      role on `<upstream-container>`:

  ```sql
  GRANT READ FILES ON EXTERNAL LOCATION `<upstream-location>` TO `<sp-app-id>`;
  ```

- [ ] Scratch schema for the acceptance test (replace `analytics.bridge_scratch`
      with `<scratch-schema>`):

  ```sql
  CREATE SCHEMA IF NOT EXISTS analytics.bridge_scratch;
  GRANT USE CATALOG ON CATALOG analytics TO `<sp-app-id>`;
  GRANT USE SCHEMA, CREATE TABLE ON SCHEMA analytics.bridge_scratch TO `<sp-app-id>`;
  ```

### Step 12 — Create the tables and grants

- [ ] Write the setup SQL:

  ```bash
  python tools/render_uc_setup.py --inventory config/inventory.dev.json \
      --output build/uc_setup.sql --job-principal <sp-app-id> --consumer-group <consumers-group>
  ```

- [ ] Open `build/uc_setup.sql`, read it, and run it in a SQL warehouse as a user
      who can create schemas and grant (a Databricks admin). It creates the log
      tables `run_log`, `table_log`, `plan`, the views `table_latest` and
      `table_current`, the target schema and every target table, and grants the
      service principal what it needs. Safe to run again.
- [ ] If any target table **existed before**, it keeps its old settings. Run, per
      such table:

  ```sql
  ALTER TABLE <target-catalog>.<target-schema>.<table>
  SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days');
  ```

- [ ] Check:

  ```sql
  SHOW TABLES IN <control-catalog>.<control-schema>;
  SHOW TABLES IN <target-catalog>.<target-schema>;
  ```

### Step 13 — Run the local tests

- [ ] From the package folder:

  ```bash
  python -m pytest tests -q
  ```

  Expect no failures (some tests are skipped when Spark or Java is not installed).

### Step 14 — Deploy the jobs to dev

- [ ] Check and select the dev inventory (creates `config/inventory.json`):

  ```bash
  python tools/prepare_deploy.py --environment dev
  ```

- [ ] Validate and deploy:

  ```bash
  databricks bundle validate -t dev
  databricks bundle deploy -t dev \
      --var job_identity=<sp-app-id> \
      --var notification_email=<ops-email> \
      --var operations_group=<ops-group> \
      --var rollback_approvers_group=<approvers-group>
  ```

- [ ] In Databricks → Jobs you now see four jobs, names starting with `[dev …]`,
      all with paused schedules:
  * *synapse-full-refresh-serverless-cetas-serverless-jdbc* — the nightly job;
  * *synapse-full-refresh-acceptance*;
  * *synapse-full-refresh-rollback*;
  * *synapse-full-refresh-maintenance*.

(Deploying through GitHub instead is described in `docs/RUNBOOK.md`, "Deploying
with GitHub Actions". The steps before and after are the same.)

### Step 15 — Acceptance test

It proves on your workspace that Delta, serverless compute, the JDBC connection
and the Synapse command path behave as the job needs. It uses only a scratch
table.

- [ ] Jobs → *synapse-full-refresh-acceptance* → **Run now with different
      parameters**:

  | Parameter | Value |
  |---|---|
  | `scratch_schema` | `<scratch-schema>` |
  | `grant_principal` | `<test-group>` |

  Or from the terminal:

  ```bash
  databricks bundle run acceptance -t dev \
      --params scratch_schema=<scratch-schema>,grant_principal=<test-group>
  ```

- [ ] The output ends with **`ALL ACCEPTANCE CHECKS PASSED`**. If not, fix what it
      reports and run it again. **Do not continue until it passes.**

---

## Part 4 — First run with 2–3 tables

### Step 16 — Know the time rules

The job only loads **today's Sydney date**.

| You start it… | Allowed until (Sydney) |
|---|---|
| without `business_date` | 07:00 |
| with `business_date=<today>` | 11:00 in prod; 20:00 in dev after Step 10 |

Any other date is refused.

### Step 17 — Choose 2–3 test tables

- [ ] In `config/inventory.dev.json`, pick 2–3 `mapping_id` values (format
      `schema__table`, e.g. `dbo__Customer`). Prefer a small table, one with a
      date/time column, and a view if you copy any.
- [ ] Write them as one list without spaces: `dbo__Customer,dbo__Region`.

### Step 18 — Put today's CSV and marker in place

If the upstream person already delivers today's files, check they exist in
`upstream-manifests/<today>/` and go to Step 19. Otherwise make test files.

- [ ] Count the rows in Synapse, per test table:

  ```sql
  SELECT COUNT_BIG(*) FROM dbo.Customer;
  SELECT COUNT_BIG(*) FROM dbo.Region;
  ```

- [ ] Create `row_counts.csv` on your laptop with today's date and those counts:

  ```
  business_date,upstream_run_id,schema,table,row_count
  2026-10-15,2026-10-15,dbo,Customer,1204331
  2026-10-15,2026-10-15,dbo,Region,12
  ```

- [ ] Create `_success_marker.json`, one line, today's date:

  ```json
  {"business_date": "2026-10-15", "upstream_run_id": "2026-10-15", "upstream_sequence": 2026101500}
  ```

- [ ] Upload the **CSV first, the marker last** (Azure portal → Storage browser →
      `<upstream-container>` → create folder `upstream-manifests/<today>` → Upload,
      or the Azure CLI):

  ```bash
  az storage fs file upload --auth-mode login --account-name <upstream-account> \
      --file-system <upstream-container> --source row_counts.csv \
      --path upstream-manifests/<today>/row_counts.csv
  az storage fs file upload --auth-mode login --account-name <upstream-account> \
      --file-system <upstream-container> --source _success_marker.json \
      --path upstream-manifests/<today>/_success_marker.json
  ```

### Step 19 — Start the run

- [ ] Jobs → *synapse-full-refresh-serverless-cetas-serverless-jdbc* → **Run now
      with different parameters**:

  | Parameter | Value |
  |---|---|
  | `business_date` | `<today>` |
  | `only_mappings` | your list from Step 17 |
  | `fail_on_any_failure` | `true` |

  From the terminal this works for **one** table (the CLI separates parameters with
  commas, so use the UI for a list):

  ```bash
  databricks bundle run nightly_full_refresh -t dev \
      --params business_date=<today>,only_mappings=dbo__Customer,fail_on_any_failure=true
  ```

- [ ] Watch the five tasks finish in order:

  | Task | What it does | Normal result |
  |---|---|---|
  | `plan` | Finds today's CSV and marker, checks Synapse objects and source columns, writes the plan | Succeeded; output shows `release_id`, `tables`, `batches` |
  | `process` | Exports each table from Synapse with CETAS and checks it | Succeeded |
  | `serverless_jdbc_fallback` | Re-reads only the tables CETAS could not export | Succeeded (does nothing when CETAS worked) |
  | `publish` | Replaces each target table in one step and verifies it | Succeeded |
  | `finalize` | Records the result and sends the summary email | Succeeded |

  If a task fails, open it, read the error, and go to Part 7.

### Step 20 — Check the results

Run in a SQL editor (`<ctl>` = `<control-catalog>.<control-schema>`):

- [ ] The run finished:

  ```sql
  SELECT event, business_date, upstream_run_id, upstream_sequence, detail
  FROM <ctl>.run_log ORDER BY event_utc DESC LIMIT 5;
  ```

  Expect `PLANNED`, then `FINISHED`.

- [ ] Every test table was published:

  ```sql
  SELECT mapping_id, status, method, source_rows, parquet_rows, written_rows,
         count_source, duration_seconds, detail
  FROM <ctl>.table_latest ORDER BY event_utc DESC;
  ```

  Expect `status = PUBLISHED`, `method = CETAS`, the three row counts equal, and
  `count_source = manifest`.

- [ ] The data is there:

  ```sql
  SELECT count(*) FROM <target-catalog>.<target-schema>.customer;
  SELECT _bridge_business_date, _bridge_release_id, count(*)
  FROM <target-catalog>.<target-schema>.customer GROUP BY ALL;
  ```

  The count equals Synapse; every row carries `<today>`.

- [ ] Compare a few rows with Synapse by hand, including a date/time column and a
      text column.

- [ ] One overwrite per table:

  ```sql
  DESCRIBE HISTORY <target-catalog>.<target-schema>.customer LIMIT 3;
  ```

  The latest entry is a `WRITE` by the service principal.

- [ ] The grants are still there:

  ```sql
  SHOW GRANTS ON TABLE <target-catalog>.<target-schema>.customer;
  ```

- [ ] The summary email arrived.

- [ ] No table needed the slower fallback route (an empty result is normal):

  ```sql
  SELECT mapping_id, error_code, detail FROM <ctl>.table_log
  WHERE status = 'FALLBACK_REQUIRED' AND event_utc > current_timestamp() - INTERVAL 1 DAY;
  ```

---

## Part 5 — Load all tables

### Step 21 — Full run

- [ ] If you made the CSV by hand, add a row for **every** table (tables missing
      from it still load, but the job counts them in Synapse, which is slower).
      Upload the CSV again first, then the marker again.
- [ ] Run the nightly job with `business_date=<today>`, `fail_on_any_failure=true`
      and `only_mappings` **empty**.
- [ ] The test tables from Part 4 show `ALREADY_CURRENT` (already loaded, not copied
      again); all others should be `PUBLISHED`:

  ```sql
  SELECT status, count(*) FROM <ctl>.table_latest
  WHERE release_id = (SELECT release_id FROM <ctl>.run_log
                      WHERE event = 'PLANNED' ORDER BY event_utc DESC LIMIT 1)
  GROUP BY status;
  ```

- [ ] Note the total run time on the job page. Starting at 01:30, it must finish
      well before 07:00.
- [ ] Force one table through the fallback route once and check it goes
      `FALLBACK_REQUIRED` → `VALIDATED` → `PUBLISHED` (see
      `docs/RELEASE_VALIDATION.md`, "Environment checks").

**Running the same day again** does not reload tables that are already loaded.
To reload changed data on the same day, the CSV needs an extra
`upstream_sequence` column with the next number (`2026101501`), a new
`upstream_run_id` (e.g. `2026-10-15-01`), and a marker with the same three
values. Otherwise test again the next day.

---

## Part 6 — Prod and go-live

### Step 22 — Prepare prod

- [ ] Repeat for the prod pool, storage and workspace:
      Steps 2–4 (Synapse, storage), Step 6 (service principal in the prod
      workspace), Step 7 (secrets in the prod workspace), Step 8 as
      `config/settings.prod.json` with the prod values, Step 10 with
      `--settings config/settings.prod.json --output config/inventory.prod.json`
      (keep `recovery_cutoff_local_time` at `11:00`), Steps 11–12 with
      `--inventory config/inventory.prod.json`.
- [ ] Deploy prod:

  ```bash
  databricks auth login --host <prod-workspace-url>
  python tools/prepare_deploy.py --environment prod
  databricks bundle validate -t prod
  databricks bundle deploy -t prod \
      --var job_identity=<sp-app-id> --var notification_email=<ops-email> \
      --var operations_group=<ops-group> --var rollback_approvers_group=<approvers-group>
  ```

- [ ] Run the acceptance test in prod (Step 15).

### Step 23 — Prod first run

- [ ] Use the **real** files from the upstream person; never hand-made ones in prod.
- [ ] Before 11:00 Sydney: Steps 19–20 with 2–3 tables, then Step 21 for all tables.

### Step 24 — Go live

- [ ] Redeploy prod with the schedules switched on:

  ```bash
  python tools/prepare_deploy.py --environment prod
  databricks bundle deploy -t prod \
      --var job_identity=<sp-app-id> --var notification_email=<ops-email> \
      --var operations_group=<ops-group> --var rollback_approvers_group=<approvers-group> \
      --var nightly_pause_status=UNPAUSED --var maintenance_pause_status=UNPAUSED
  ```

  Do not switch the schedule on only in the Databricks UI: the next deploy would
  pause it again.
- [ ] Unless predictive optimization is on in your workspace, set the maintenance
      job's `vacuum` parameter to `true`.

From now on the nightly job runs by itself at 01:30 Sydney every day and the
maintenance job every Sunday at 14:00. Each morning, read the summary email; if
anything failed, use Part 7.

---

## Part 7 — If something fails

| Where / message | Meaning | What to do |
|---|---|---|
| `plan`: *only_mappings has unknown or disabled ids* | Typo in `only_mappings` | Copy the ids exactly from the inventory |
| `plan`: *only repairs today's Sydney business date* | `business_date` is not today in Sydney | Use today's Sydney date |
| `plan`: *cutoff … has passed* | Too late in the day | Dev: Step 10's later cutoff, redeploy. Otherwise wait until tomorrow morning |
| `plan`: *No COMPLETE upstream … no manifest found* | CSV or marker not found | Check the folder date, both file names, `manifest.path_template`, `manifest.success_marker` |
| `plan`: *manifest marker does not certify the CSV* | Marker values differ from the CSV | Same date in both; `upstream_sequence` a number ending in `00` |
| `plan`: *manifest is missing column(s)* / *mixes business dates* | CSV layout wrong | Fix the header or rows (Step 18) |
| `plan`: secret, token or *Login failed* error | Secret, permission or Synapse user | Step 7 (keys, `READ` for `<sp-app-id>`) and Step 3 (user `<sp-name>`) |
| `plan`: *must be PARQUET* / data source location error | Synapse staging objects don't match the settings | `staging.*` in Step 8 must match Step 3 |
| `FAILED_TARGET_SCHEMA` | Target table missing or different from the inventory | Run `build/uc_setup.sql` (Step 12); never loosen the check |
| `FAILED_SOURCE_ACCESS` | Synapse refused a permission | Recheck Step 3's grants |
| `FAILED_SCHEMA_DRIFT` | Source columns changed since Step 10 | Regenerate that table's entry |
| `FAILED_CONTRACT` | Synapse wrote a data type the job did not expect | Note table and detail; see `docs/RUNBOOK.md` |
| `FAILED_COUNT_MISMATCH` | Exported rows differ from the expected count | Check the CSV count; run again |
| `REJECTED_EMPTY` | 0 rows and `allow_empty` is false | Confirm with the upstream person; set `allow_empty` if empty is normal |
| `FAILED_EXPORT` | Export or connection error (already retried once) | Read `detail`, fix, run again |

After a fix, start a new run with the same parameters (Step 19). Tables that
already succeeded show `ALREADY_CURRENT` and are not copied again. More cases:
`docs/RUNBOOK.md`, "Common situations".
