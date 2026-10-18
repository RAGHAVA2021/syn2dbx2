# The service principal (`sp-bridge`) and the Synapse setup scripts — questions and answers

A record of the questions asked while setting up the project, in the order they
were asked, with the answers. It explains the two Synapse scripts
(`sql/synapse/03_review_credential_scope.sql` and `sql/synapse/01_export_setup.sql`),
the objects they create, and what the service principal does in each system.

`sp-bridge` is the example display name used throughout. Replace it with your
service principal's real display name.

Contents:

1. Which script runs first, and what are their inputs and outputs?
2. What is `<databricks-job-identity>`, and what does "the output of 01 feeds `settings.json`" mean?
3. What are `bridge_meta`, `bridge_stage` and `bridge_msi`?
4. Is `<databricks-job-identity>` the service principal's display name, registered as a Synapse user?
5. What are the roles of `sp-bridge`?
6. Is this the whole flow, from the Databricks job to the tables?

---

## 1. Which script runs first, and what are their inputs and outputs?

**Question:** As per the documents, first I need to run `01_export_setup`, then
`03_review_credential_scope`, right? What is the input for these two files, and
the output of these two files?

**Answer:** Not quite: run **`03_review_credential_scope.sql` first**, then
`01_export_setup.sql`. This is the order in `FIRST_RUN.md` (Step 2, then
Step 3). The review comes first because `01` grants a permission,
`ALTER ANY EXTERNAL DATA SOURCE`, that can reach every credential in the pool.
Someone should approve that before it is granted.

Both run in Synapse Studio or SSMS, connected to the dedicated pool, as a pool
administrator.

### `03_review_credential_scope.sql` (run first)

| | |
|---|---|
| **Input** | Nothing. It has no placeholders, so run it as it is. |
| **What it does** | Two read-only `SELECT`s. It changes nothing. |
| **Output** | Two result tables on screen, which you can copy out. Nothing is saved anywhere. |

The two results:

1. Every database-scoped credential in the pool: `name`, `credential_identity`.
2. Every external data source: `external_data_source`, `location`, and the
   credential it uses.

You show these to the data owner and ask: "This job's identity will be able to
reach all of these credentials. Is that acceptable?" The approval is the real
output.

#### `03` in detail: every input and output, and its purpose

**Inputs**

| # | Input | Purpose |
|---|---|---|
| 1 | Your login to the pool (a Synapse administrator) | The script reads the pool's system views, which only an administrator can see. It needs no other setup. |
| 2 | The pool's system view `sys.database_scoped_credentials` | The list of credentials stored in the pool. This is what the review is about. |
| 3 | The pool's system view `sys.external_data_sources` (joined to the credentials) | Shows which storage locations exist and which credential each one uses, so you can spot unrelated ones. |

There are no placeholders to replace and nothing to configure.

**Outputs**

| # | Output | Purpose |
|---|---|---|
| 1 | Result table 1: credential `name` and `credential_identity` | Shows every identity the pool can use to reach other systems. These are what `ALTER ANY EXTERNAL DATA SOURCE` can reach. |
| 2 | Result table 2: `external_data_source`, `location`, `database_scoped_credential` | Shows which storage each data source points at and with which credential. |
| 3 | The data owner's approval (a human decision, not a file) | The gate before `01` runs. Without it, do not grant the permission in `01`. |

The script's own comments add two rules: do not paste this output into routine
job logs (names and locations reveal the scope of the pool's credentials), and
also check role membership and inherited permissions for the actual job
identity. A grant on only `bridge_meta` does not narrow `ALTER ANY EXTERNAL DATA
SOURCE`. If unrelated credentials share the database, use a separately isolated
database or pool, or get explicit security approval for the full reach.

### `01_export_setup.sql` (run second, only after approval)

| | |
|---|---|
| **Input** | You edit the script first and replace four placeholders, everywhere they appear: `<staging-container>`, `<storage-account>`, `<databricks-job-identity>` (the service principal's display name) and `<source-schema>`. |
| **Needs in place** | The service principal exists in Entra ID. The Synapse workspace managed identity has *Storage Blob Data Contributor* on the staging container (`FIRST_RUN.md`, Step 4). |
| **What it does** | Creates only what is missing, then grants permissions. |
| **Output** | Objects and permissions in the pool, plus "Commands completed" messages. There is no data result. |

What it leaves behind in Synapse:

| Created | Name |
|---|---|
| Schema | `bridge_meta` |
| Master key | (system) |
| Credential | `bridge_msi` |
| External data source | `bridge_stage` |
| External file format | `bridge_parquet` |
| Database user | the service principal |
| Grants | `SELECT` on the source schema, `CREATE TABLE`, `ALTER` on `bridge_meta`, `ALTER ANY EXTERNAL DATA SOURCE`, `ALTER ANY EXTERNAL FILE FORMAT`, `ADMINISTER DATABASE BULK OPERATIONS` |

If a placeholder is left, the script stops with
`Replace all placeholders before running this script.` and creates nothing.

The output of `01` feeds `config/settings.json`: its object names become
`staging.metadata_schema`, `staging.external_data_source` and
`staging.file_format` (see question 2).

To check the result:

```sql
SELECT name, location FROM sys.external_data_sources WHERE name = 'bridge_stage';
SELECT name FROM sys.database_principals WHERE name = '<sp-name>';
```

#### `01` in detail: every input and output, and its purpose

**Inputs you provide**

| # | Input | Purpose |
|---|---|---|
| 1 | `<staging-container>` | The container name inside the `LOCATION` of data source `bridge_stage`. It is where the export writes the Parquet files. Must equal the container in `staging.root`. |
| 2 | `<storage-account>` | The storage account in that same `LOCATION`. Must equal the account in `staging.root`. |
| 3 | `<databricks-job-identity>` (the service principal's display name) | The name of the database user the script creates, and the user every `GRANT` goes to. It is how Synapse recognizes the job's login. |
| 4 | `<source-schema>` | The schema whose tables and views are copied. The user gets `SELECT` on it. For several schemas, repeat that `GRANT` line once per schema. |
| 5 | Your login to the pool (a Synapse administrator) | Creating schemas, credentials, data sources and users, and granting permissions, are administrator actions. |

**Inputs that must already exist**

| # | Input | Purpose |
|---|---|---|
| 6 | The service principal in Entra ID | `CREATE USER ... FROM EXTERNAL PROVIDER` looks the name up there. If it is missing, that statement fails. |
| 7 | *Storage Blob Data Contributor* for the Synapse workspace managed identity on the staging container | This is what lets Synapse write the files. The script cannot grant it; it is done in Azure. Without it, the export fails at write time. |
| 8 | The pool's current state | Every step is `IF NOT EXISTS`, so rerunning is safe and existing objects are kept. |

**Fixed values inside the script** (not inputs; change them only if you also change
`settings.json`): the names `bridge_meta`, `bridge_msi`, `bridge_stage`,
`bridge_parquet`, the data source type `HADOOP`, Snappy compression, and the
credential identity `'Managed Service Identity'`.

**Outputs**

| # | Output | Purpose |
|---|---|---|
| 1 | Schema `bridge_meta` | Holds the temporary CETAS tables of each export. Used by `staging.metadata_schema`. |
| 2 | Database master key | Synapse requires one to store a database-scoped credential. |
| 3 | Credential `bridge_msi` | How Synapse signs in to storage: as its own workspace managed identity, with no password stored. |
| 4 | External data source `bridge_stage` | Points at the staging container; every export names it. Used by `staging.external_data_source`. The plan checks its location equals `staging.root` each night. |
| 5 | External file format `bridge_parquet` | Tells Synapse to write Parquet, so special characters in the data cannot break the files. Used by `staging.file_format`; the plan fails the night if it is not PARQUET. |
| 6 | Database user `[<databricks-job-identity>]` | Lets the service principal log in to the pool. |
| 7 | `SELECT` on the source schema | Read the tables and views to copy, and their column definitions. |
| 8 | `CREATE TABLE` | Create the temporary CETAS table for each export. |
| 9 | `ALTER` on schema `bridge_meta` | Create and drop those temporary tables, in that schema only. |
| 10 | `ALTER ANY EXTERNAL DATA SOURCE` | Use `bridge_stage` in an export. It reaches all credentials, which is why `03` is run first. |
| 11 | `ALTER ANY EXTERNAL FILE FORMAT` | Use the Parquet format in an export. |
| 12 | `ADMINISTER DATABASE BULK OPERATIONS` | Run bulk export commands such as CETAS. |
| 13 | Messages: "Commands completed", or `Replace all placeholders before running this script.` | Success, or a stop before anything is created if a placeholder was left. |

**Optional, not created by default:** the last section of the script is a
commented-out workload group and classifier (`bridge_export`). Uncomment and size
it only if the nightly export must be kept from starving other users of the pool.
It also uses `<databricks-job-identity>`. Keep the job's `export_concurrency` at or
below what that group allows.

**How the two scripts connect:** the outputs of `03` (the lists and the approval)
decide whether you run `01`. The outputs of `01` (object names) are typed into
`config/settings.json`, and its user and grants are what the nightly job's login
relies on.

---

## 2. What is `<databricks-job-identity>`, and what does "the output of 01 feeds `settings.json`" mean?

**Question:** What is `<databricks-job-identity>`? You said service principal
display name. You said "The output of 01 feeds `config/settings.json`"; what does
it mean?

### `<databricks-job-identity>`

It is the name of the **service principal as Synapse knows it**: the name of the
database user that gets the export permissions.

- **Where to find it:** Azure portal → Microsoft Entra ID → App registrations →
  your service principal's app → **Display name** on the Overview page. For
  example, `sp-bridge`. It is the readable name, **not** the long application
  (client) ID like `1234abcd-…`.
- **What the script does with it:**

  ```sql
  CREATE USER [sp-bridge] FROM EXTERNAL PROVIDER;
  GRANT SELECT ON SCHEMA::[dbo] TO [sp-bridge];
  ...
  ```

  Synapse looks up that name in Entra ID and creates a database user for it. The
  grants then give that user the permissions to read the source tables and run
  CETAS. Without the user, the job's login to Synapse is accepted but has no
  rights.
- **Replace it in every place** it appears in the script, keeping the square
  brackets: `[sp-bridge]`.

It is the same service principal in two places, named differently:

| Where | Name used |
|---|---|
| Synapse (`01_export_setup.sql`) | **Display name**, e.g. `sp-bridge` |
| Databricks (`--var job_identity`, `--job-principal`, secrets ACL) | **Application ID**, e.g. `1234abcd-…` |

If two Entra objects have the same display name, `CREATE USER` can fail. Then ask
your Azure admin for a unique name.

### "The output of 01 feeds `settings.json`"

The script **does not write anything to `settings.json`**. It creates objects in
Synapse with certain names, and the job has to be told those names. You type them
into `settings.json` yourself:

| Created by `01_export_setup.sql` (Synapse) | You enter it in `config/settings.json` under |
|---|---|
| schema `bridge_meta` | `staging.metadata_schema` |
| data source `bridge_stage` | `staging.external_data_source` |
| file format `bridge_parquet` | `staging.file_format` |

The example settings already contain exactly those three names, so if you keep
the defaults in the script, you change nothing. You only edit them in
`settings.json` if you changed the names in the script, or reused existing
objects. Each night the job checks that these objects exist and match, and fails
before touching any table if they do not.

---

## 3. What are `bridge_meta`, `bridge_stage` and `bridge_msi`?

**Question:** What is Schema `bridge_meta`, External data source `bridge_stage`,
Credential `bridge_msi`?

Three objects that are all created inside the Synapse pool by
`01_export_setup.sql`. Each has one job in the export.

### Schema `bridge_meta`

A **schema** is a folder inside the Synapse database that holds tables.
`bridge_meta` is a folder just for the bridge.

Every night, for each table, the job creates a **temporary CETAS table** in
Synapse such as `bridge_meta.x_dbo__Customer_<attempt>`. This is a side effect of
how CETAS works: the export command creates a table whose definition points at the
Parquet files it wrote. The job drops it afterwards, and the weekly maintenance
job removes any left behind by a crash.

Your own tables are never touched. The job identity has `ALTER` permission only on
`bridge_meta`, so the temporary tables cannot mix with your real ones.

### External data source `bridge_stage`

Synapse's saved pointer to the staging container: "when I say `bridge_stage`, I
mean `abfss://<staging-container>@<storage-account>.dfs.core.windows.net`, and I
will sign in with credential `bridge_msi`."

The CETAS command names it, so Synapse knows where to write the Parquet files.
Databricks later reads the same container.

### Credential `bridge_msi`

The **login** Synapse uses when it writes to the storage account. The script
creates it as:

```sql
CREATE DATABASE SCOPED CREDENTIAL bridge_msi WITH IDENTITY = 'Managed Service Identity';
```

That means "use the Synapse workspace's own managed identity". No password or key
is stored. That identity needs the *Storage Blob Data Contributor* role on the
staging container (`FIRST_RUN.md`, Step 4), or the write fails.

### How they fit together

```
CETAS export of dbo.Customer
  │  creates a temp table in    → bridge_meta         (the folder for temp tables)
  │  writes files through       → bridge_stage        (points at the staging container)
  │  which signs in with        → bridge_msi          (the Synapse managed identity)
  │  and writes as              → bridge_parquet      (the file format)
  ▼
abfss://staging@acct.../bridge-stage/<date>/.../dbo__Customer/*.parquet
```

The credential is also why the security review comes first.
`ALTER ANY EXTERNAL DATA SOURCE`, which the job identity needs, can reach
**every** credential in the pool, not just `bridge_msi`.
`03_review_credential_scope.sql` lists them all so the data owner can approve
that.

---

## 4. Is `<databricks-job-identity>` the service principal's display name, registered as a Synapse user?

**Question:** So `<databricks-job-identity>` is a service principal display name;
we assign permission for the service principal as a user to log in to Synapse?

**Answer:** Yes, that is right. In `01_export_setup.sql`:

1. **`CREATE USER [sp-bridge] FROM EXTERNAL PROVIDER`** registers the service
   principal as a **database user** in the Synapse pool, so the pool recognizes
   it.
2. **The `GRANT` lines** give that user its permissions: read the source tables
   and run CETAS.

**How the login works each night** (`client_secret` mode):

1. The job reads the tenant ID, client ID and client secret from the Databricks
   secret scope.
2. It uses them to get an Entra ID token for the service principal.
3. It connects to Synapse with that token.
4. Synapse matches the token to the database user `sp-bridge` and applies its
   permissions.

If the user does not exist in the pool, the login fails. If it exists but lacks
the grants, the login works but the export fails with a permission error
(`FAILED_SOURCE_ACCESS`).

**One thing to keep clear:** `sp-bridge` is the *display name* in Synapse, but the
job logs in with the *application ID and secret*. Both belong to the same service
principal. The script only needs the display name, because `FROM EXTERNAL
PROVIDER` looks the principal up by name in Entra ID.

---

## 5. What are the roles of `sp-bridge`?

**Question:** I did not understand the roles of `sp-bridge`. All I know is it is a
service principal, and we are registering it as a user in the Synapse pool; then
what are its roles?

**Answer:** `sp-bridge` is one identity that does a different job in each system.
In Synapse, its roles are the permissions granted to it by
`01_export_setup.sql`.

### In Synapse

These six permissions let it do two things: **read** the source tables and
**export** them.

| Permission | What it lets `sp-bridge` do |
|---|---|
| `SELECT` on the source schema | Read the tables and views to be copied, and their column definitions |
| `CREATE TABLE` | Create the temporary CETAS table for each export |
| `ALTER` on schema `bridge_meta` | Create and drop those temporary tables, in that schema only |
| `ALTER ANY EXTERNAL DATA SOURCE` | Use the `bridge_stage` pointer in an export (this one reaches all credentials, which is why `03_review_credential_scope.sql` is run first) |
| `ALTER ANY EXTERNAL FILE FORMAT` | Use the Parquet format in an export |
| `ADMINISTER DATABASE BULK OPERATIONS` | Run bulk export commands like CETAS |

It cannot modify or delete your source tables, or change anything outside
`bridge_meta`.

### In the other systems

The same service principal is also what Databricks runs the jobs as. Everything
it needs there is set up in the later steps:

| System | What `sp-bridge` does | Set up in |
|---|---|---|
| **Databricks jobs** | The nightly, acceptance, rollback and maintenance jobs all run as it (`job_identity`) | Deploy step |
| **Databricks secrets** | Reads its own tenant ID, client ID and client secret, and the webhook URL | Secrets step (`READ` on the scope) |
| **Unity Catalog** | Reads and writes your target tables (`SELECT, MODIFY`), writes the log tables, uses the JDBC connection, and reads and writes the staging files | `uc_setup.sql` and the manual grants |
| **Azure storage** | Reads the daily CSV and marker, read-only, through a Unity Catalog external location | The external location grant |

### One picture

```
sp-bridge (one service principal)
 ├─ runs the Databricks jobs
 ├─ logs in to Synapse → its Synapse permissions above: read sources, run exports
 ├─ reads/writes Delta tables and files in Unity Catalog
 └─ reads its secrets from the secret scope
```

So "registering it as a user in Synapse" is only one of its four places. Without
it in Synapse, the exports fail. Without the other three, the jobs cannot run,
cannot read their secrets, or cannot write your tables.

---

## 6. Is this the whole flow, from the Databricks job to the tables?

**Question:** So this service principal runs the job from Databricks, as it is
registered as a user in Synapse and has permissions; so from Databricks it
triggers jobs, then logs in to Synapse, then runs CETAS and loads data into
storage, then reads from storage and creates tables in Databricks, right?

**Answer:** Yes, you have the flow right. Three details to correct:

1. **The schedule starts the job, not the service principal.** The nightly job
   starts by itself at 01:30, and `sp-bridge` is the identity it runs as.
   Everything the job does carries `sp-bridge`'s permissions.
2. **Synapse writes the files into storage, not Databricks.** `sp-bridge` only
   sends the CETAS command. The export then runs inside Synapse, which writes to
   storage with its own workspace managed identity (`bridge_msi`), not with
   `sp-bridge`'s access.
3. **The job does not create the tables.** It overwrites existing target tables
   with the new data. The tables were created once, earlier, by `uc_setup.sql`.

### The flow, with who acts at each point

```
01:30  Databricks schedule starts the nightly job → runs as sp-bridge

 plan      sp-bridge reads today's CSV + marker from storage (read-only)
           sp-bridge logs in to Synapse (token from its secrets) → checks source columns

 process   sp-bridge sends "CREATE EXTERNAL TABLE AS SELECT ..." to Synapse
           └─ Synapse runs it inside the pool and writes Parquet files
              to the staging container (as the Synapse managed identity, bridge_msi)
           Databricks (sp-bridge) reads those files from storage and checks them
           (columns, types, row count against the CSV)

 fallback  only if CETAS failed for a table: sp-bridge reads it over the UC JDBC
           connection and writes the Parquet itself

 publish   sp-bridge runs INSERT OVERWRITE into the existing target table
           (one commit), then checks the result in the Delta history

 finalize  records the night in the log tables and sends the summary email
```

### What can fail, and which setup step fixes it

| What fails | Because | Setup step |
|---|---|---|
| Login to Synapse | `sp-bridge` is not a Synapse user, or its secrets are wrong | Synapse setup script, secrets |
| The export | The Synapse permissions are missing | Synapse setup script |
| The files cannot be written | The Synapse managed identity lacks *Storage Blob Data Contributor* | Storage step |
| The files cannot be read, or the table cannot be written | `sp-bridge` lacks the Unity Catalog grants | UC grants |
