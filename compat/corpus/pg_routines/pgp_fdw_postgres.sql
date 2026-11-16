-- case: pgp_fdw_postgres
-- title: Postgres foreign server mapping and table DDL without connection
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE EXTENSION postgres_fdw;
CREATE SERVER r0c_pgp_fdw_postgres_srv TYPE 'postgres' VERSION '18' FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1',port '1',dbname 'postgres',connect_timeout '1');
ALTER SERVER r0c_pgp_fdw_postgres_srv VERSION '18.6' OPTIONS (ADD fetch_size '100');
CREATE USER MAPPING FOR CURRENT_USER SERVER r0c_pgp_fdw_postgres_srv OPTIONS (user 'postgres');
ALTER USER MAPPING FOR CURRENT_USER SERVER r0c_pgp_fdw_postgres_srv OPTIONS (SET user 'application');
-- primary-key exception: foreign tables do not support primary key constraints.
CREATE FOREIGN TABLE r0c_pgp_fdw_postgres(id integer, label text) SERVER r0c_pgp_fdw_postgres_srv OPTIONS (schema_name 'public',table_name 'remote_items');
ALTER FOREIGN TABLE r0c_pgp_fdw_postgres ADD COLUMN extra integer;
ALTER FOREIGN TABLE r0c_pgp_fdw_postgres OPTIONS (SET table_name 'items');
SELECT c.relname,f.ftoptions,s.srvname,s.srvversion,s.srvoptions FROM pg_foreign_table f JOIN pg_class c ON c.oid=f.ftrelid JOIN pg_foreign_server s ON s.oid=f.ftserver WHERE c.relname='r0c_pgp_fdw_postgres';
SELECT usename,srvname,umoptions FROM pg_user_mappings WHERE srvname='r0c_pgp_fdw_postgres_srv' ORDER BY usename;
DROP FOREIGN TABLE r0c_pgp_fdw_postgres;
DROP USER MAPPING FOR CURRENT_USER SERVER r0c_pgp_fdw_postgres_srv;
DROP SERVER r0c_pgp_fdw_postgres_srv;
DROP EXTENSION postgres_fdw;
