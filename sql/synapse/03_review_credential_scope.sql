-- Read-only security review, run as a Synapse dedicated-pool administrator.
-- Do not send this output to routine job logs: names and storage locations
-- reveal the scope of the pool's database-scoped credentials.
-- Review EVERY credential and data source. ALTER ANY EXTERNAL DATA SOURCE
-- gives the export principal access to all database-scoped credentials,
-- including ones unrelated to this bridge. This review is not a permission
-- boundary: new credentials created later also become reachable.

SELECT name, credential_identity
FROM sys.database_scoped_credentials
ORDER BY name;

SELECT ds.name AS external_data_source, ds.location,
       c.name AS database_scoped_credential
FROM sys.external_data_sources AS ds
LEFT JOIN sys.database_scoped_credentials AS c
  ON ds.credential_id = c.credential_id
ORDER BY ds.name;

-- Also inspect role membership, inherited permissions, and effective
-- principal rights for the actual job identity. If unrelated credentials
-- share this database, use a separately isolated export database/pool or
-- obtain explicit security approval for the full credential reach. A grant
-- on only bridge_meta does not narrow ALTER ANY EXTERNAL DATA SOURCE.
