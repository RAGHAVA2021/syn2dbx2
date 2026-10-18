# Infrastructure notes

## Staging retention (`adls_lifecycle_policy.json`)
Deletes CETAS Parquet files 3 days after they were written. Replace `<container>`
and keep `bridge-stage` equal to `staging.prefix` in the inventory. Apply with:

    az storage account management-policy create \
        --account-name <account> --resource-group <rg> \
        --policy @infra/adls_lifecycle_policy.json

The nightly job never reads staging files older than the current night, so any
value of 1 day or more is safe; 3 days leaves time to investigate a failure.
On hierarchical-namespace accounts the lifecycle rule removes files; empty
folders may remain and cost nothing meaningful.

## Identities and access (summary)
The environment is already connected. Use this table to **verify** existing access,
not to create new identities (see AGENTS.md section 2).

| Identity | Needs |
|---|---|
| Synapse workspace managed identity | Storage Blob Data Contributor on the staging container (CETAS writes) |
| Databricks job identity (service principal) | Synapse user with the grants in `sql/synapse/01_export_setup.sql`; UC grants from `tools/render_uc_setup.py`; READ FILES and WRITE FILES on the staging external location (WRITE is for the JDBC fallback); READ FILES on the upstream manifest location |
| UC service credential (if `auth.mode = service_credential`) | Access connector managed identity added to Synapse as the database user above |

The manifest and its success marker must reside in another container where the
job identity has read-only effective storage RBAC/ACLs. The inventory rejects a
manifest in the job-writable staging container. Check inherited storage-account
RBAC too. A separate staging account and a dedicated Synapse managed identity
reduce the reach of Synapse's database-wide external-data-source privileges.
Audit changes to external data sources, external file formats and credentials.

Staging files are full unmasked source copies; restrict READ FILES and container
RBAC to the bridge and authorized responders. Successful stages are removed
after the verified publish; incomplete attempts remain for incident analysis
until the three-day lifecycle rule deletes them.

## Compute
The nightly and acceptance jobs run on serverless compute as the job identity.
CETAS and other command SQL use `mssql-python` (installed by the job
environment in `databricks.yml`), so no JVM JDBC connection is needed; the
fallback reads through the Unity Catalog JDBC connection, which needs
`USE CONNECTION` for the job identity. Serverless compute needs network access
to the Synapse SQL endpoint and the storage accounts.

Only the operator maintenance and rollback jobs use job clusters, in dedicated
(single-user) access mode as the job identity.
