-- case: pgp_fdw_import
-- title: IMPORT FOREIGN SCHEMA unreachable endpoint SQLSTATE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE EXTENSION postgres_fdw;
CREATE SCHEMA r0c_pgp_fdw_import;
CREATE SERVER r0c_pgp_fdw_import_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1',port '1',connect_timeout '1');
CREATE USER MAPPING FOR CURRENT_USER SERVER r0c_pgp_fdw_import_srv OPTIONS (user 'postgres');
-- @error
IMPORT FOREIGN SCHEMA public LIMIT TO (items) FROM SERVER r0c_pgp_fdw_import_srv INTO r0c_pgp_fdw_import;
DROP USER MAPPING FOR CURRENT_USER SERVER r0c_pgp_fdw_import_srv;
DROP SERVER r0c_pgp_fdw_import_srv;
DROP SCHEMA r0c_pgp_fdw_import;
DROP EXTENSION postgres_fdw;
