-- case: pgp_fdw_file
-- title: File foreign table DDL and catalogs avoid host dependent contents
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE EXTENSION file_fdw;
CREATE SERVER r0c_pgp_fdw_file_srv FOREIGN DATA WRAPPER file_fdw;
-- primary-key exception: foreign tables do not support primary key constraints.
CREATE FOREIGN TABLE r0c_pgp_fdw_file(line text) SERVER r0c_pgp_fdw_file_srv OPTIONS (filename '/etc/hostname',format 'text');
ALTER FOREIGN TABLE r0c_pgp_fdw_file OPTIONS (ADD delimiter '|');
SELECT c.relname, f.ftoptions FROM pg_foreign_table f JOIN pg_class c ON c.oid=f.ftrelid WHERE c.relname='r0c_pgp_fdw_file';
DROP FOREIGN TABLE r0c_pgp_fdw_file;
DROP SERVER r0c_pgp_fdw_file_srv;
DROP EXTENSION file_fdw;
