/* ============================================================================
   01 · Export objects in the Synapse dedicated SQL pool (run once per pool)
   Replace every <placeholder>. Run as a pool administrator.

   CETAS writes Parquet into the staging container using the Synapse workspace
   managed identity, which needs "Storage Blob Data Contributor" on that
   container (or on the storage account).
   ============================================================================ */

IF '<staging-container>' LIKE '<%>' OR '<storage-account>' LIKE '<%>'
   OR '<databricks-job-identity>' LIKE '<%>' OR '<source-schema>' LIKE '<%>'
    THROW 50000, 'Replace all placeholders before running this script.', 1;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bridge_meta')
    EXEC('CREATE SCHEMA bridge_meta');
GO

IF '<staging-container>' LIKE '<%>' OR '<storage-account>' LIKE '<%>'
   OR '<databricks-job-identity>' LIKE '<%>' OR '<source-schema>' LIKE '<%>'
    THROW 50000, 'Replace all placeholders before running this script.', 1;

IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY;
GO

IF '<staging-container>' LIKE '<%>' OR '<storage-account>' LIKE '<%>'
   OR '<databricks-job-identity>' LIKE '<%>' OR '<source-schema>' LIKE '<%>'
    THROW 50000, 'Replace all placeholders before running this script.', 1;

IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'bridge_msi')
    CREATE DATABASE SCOPED CREDENTIAL bridge_msi WITH IDENTITY = 'Managed Service Identity';
GO

IF '<staging-container>' LIKE '<%>' OR '<storage-account>' LIKE '<%>'
   OR '<databricks-job-identity>' LIKE '<%>' OR '<source-schema>' LIKE '<%>'
    THROW 50000, 'Replace all placeholders before running this script.', 1;

IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'bridge_stage')
    CREATE EXTERNAL DATA SOURCE bridge_stage WITH (
        TYPE = HADOOP,
        LOCATION = 'abfss://<staging-container>@<storage-account>.dfs.core.windows.net',
        CREDENTIAL = bridge_msi);
GO

IF '<staging-container>' LIKE '<%>' OR '<storage-account>' LIKE '<%>'
   OR '<databricks-job-identity>' LIKE '<%>' OR '<source-schema>' LIKE '<%>'
    THROW 50000, 'Replace all placeholders before running this script.', 1;

IF NOT EXISTS (SELECT 1 FROM sys.external_file_formats WHERE name = 'bridge_parquet')
    CREATE EXTERNAL FILE FORMAT bridge_parquet WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec');
GO

IF '<staging-container>' LIKE '<%>' OR '<storage-account>' LIKE '<%>'
   OR '<databricks-job-identity>' LIKE '<%>' OR '<source-schema>' LIKE '<%>'
    THROW 50000, 'Replace all placeholders before running this script.', 1;

/* ---------------------------------------------------------------------------
   The Databricks job identity (service principal or access-connector managed
   identity). Use the display name of the Entra object.
   --------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '<databricks-job-identity>')
    CREATE USER [<databricks-job-identity>] FROM EXTERNAL PROVIDER;
GO

IF '<staging-container>' LIKE '<%>' OR '<storage-account>' LIKE '<%>'
   OR '<databricks-job-identity>' LIKE '<%>' OR '<source-schema>' LIKE '<%>'
    THROW 50000, 'Replace all placeholders before running this script.', 1;

-- Read the approved sources (repeat per source schema).
GRANT SELECT ON SCHEMA::[<source-schema>] TO [<databricks-job-identity>];

-- CETAS: create/drop external tables in bridge_meta using the data source and format.
-- SECURITY GATE: before granting ALTER ANY EXTERNAL DATA SOURCE, run
-- 03_review_credential_scope.sql as a pool administrator. Microsoft documents
-- that this permission also grants reach to EVERY database-scoped credential
-- in this database, not only bridge_msi. A shared pool with unrelated scoped
-- credentials needs an approved isolation decision before using this identity.
-- These are the permissions Microsoft documents for CETAS; confirm on your pool.
GRANT CREATE TABLE TO [<databricks-job-identity>];
GRANT ALTER ON SCHEMA::[bridge_meta] TO [<databricks-job-identity>];
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [<databricks-job-identity>];
GRANT ALTER ANY EXTERNAL FILE FORMAT TO [<databricks-job-identity>];
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [<databricks-job-identity>];
GO

/* ---------------------------------------------------------------------------
   Optional but recommended: isolate export load with workload management so
   the nightly export cannot starve other pool users. Size the percentages to
   your DWU; the for-each concurrency in databricks.yml should not exceed the
   concurrency this group allows.
   --------------------------------------------------------------------------- */
-- CREATE WORKLOAD GROUP bridge_export WITH (
--     MIN_PERCENTAGE_RESOURCE = 0,
--     CAP_PERCENTAGE_RESOURCE = 40,
--     REQUEST_MIN_RESOURCE_GRANT_PERCENT = 5);
-- CREATE WORKLOAD CLASSIFIER bridge_export_classifier WITH (
--     WORKLOAD_GROUP = 'bridge_export',
--     MEMBERNAME = '<databricks-job-identity>');
