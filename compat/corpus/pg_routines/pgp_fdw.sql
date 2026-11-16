-- case: pgp_fdw
-- title: Custom foreign data wrapper lifecycle
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE EXTENSION postgres_fdw;
CREATE FOREIGN DATA WRAPPER r0c_pgp_fdw HANDLER postgres_fdw_handler VALIDATOR postgres_fdw_validator;
ALTER FOREIGN DATA WRAPPER r0c_pgp_fdw NO VALIDATOR;
ALTER FOREIGN DATA WRAPPER r0c_pgp_fdw OPTIONS (ADD application_tag 'corpus');
SELECT fdwname,fdwoptions FROM pg_foreign_data_wrapper WHERE fdwname='r0c_pgp_fdw';
ALTER FOREIGN DATA WRAPPER r0c_pgp_fdw OWNER TO postgres;
DROP FOREIGN DATA WRAPPER r0c_pgp_fdw;
DROP EXTENSION postgres_fdw;
