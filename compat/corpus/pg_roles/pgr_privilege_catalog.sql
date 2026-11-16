-- case: pgr_privilege_catalog
-- title: Privilege helpers information_schema and exploded ACLs
-- owner: unowned
-- expect: compatible
-- source: Roles: has_*_privilege role_table_grants table_privileges relacl aclexplode
CREATE ROLE r0c_pgr_privilege_catalog_u;
CREATE SCHEMA r0c_pgr_privilege_catalog;
CREATE TABLE r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t (id integer PRIMARY KEY, value text);
CREATE SEQUENCE r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_s;
CREATE FUNCTION r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_f() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
REVOKE EXECUTE ON FUNCTION r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_f() FROM PUBLIC;
GRANT USAGE ON SCHEMA r0c_pgr_privilege_catalog TO r0c_pgr_privilege_catalog_u;
GRANT SELECT ON r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t TO r0c_pgr_privilege_catalog_u;
GRANT UPDATE (value) ON r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t TO r0c_pgr_privilege_catalog_u;
GRANT USAGE ON SEQUENCE r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_s TO r0c_pgr_privilege_catalog_u;
GRANT EXECUTE ON FUNCTION r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_f() TO r0c_pgr_privilege_catalog_u;
SET ROLE r0c_pgr_privilege_catalog_u;
SELECT has_table_privilege('r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t', 'SELECT'), has_table_privilege('r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t', 'DELETE'), has_column_privilege('r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t', 'value', 'UPDATE'), has_any_column_privilege('r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t', 'UPDATE'), has_schema_privilege('r0c_pgr_privilege_catalog', 'USAGE'), has_schema_privilege('r0c_pgr_privilege_catalog', 'CREATE'), has_sequence_privilege('r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_s', 'USAGE'), has_function_privilege('r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_f()', 'EXECUTE'), has_database_privilege(current_database(), 'CONNECT');
SELECT grantor, grantee, table_schema, table_name, privilege_type, is_grantable FROM information_schema.role_table_grants WHERE table_schema = 'r0c_pgr_privilege_catalog' ORDER BY grantor, grantee, table_schema, table_name, privilege_type;
SELECT grantor, grantee, table_schema, table_name, privilege_type, is_grantable FROM information_schema.table_privileges WHERE table_schema = 'r0c_pgr_privilege_catalog' ORDER BY grantor, grantee, table_schema, table_name, privilege_type;
RESET ROLE;
SELECT c.relname, pg_get_userbyid(a.grantor) AS grantor, CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee, a.privilege_type, a.is_grantable FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE n.nspname = 'r0c_pgr_privilege_catalog' ORDER BY c.relname, grantor, grantee, a.privilege_type, a.is_grantable;
DROP FUNCTION r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_f();
DROP SEQUENCE r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_s;
DROP TABLE r0c_pgr_privilege_catalog.r0c_pgr_privilege_catalog_t;
DROP SCHEMA r0c_pgr_privilege_catalog;
DROP ROLE r0c_pgr_privilege_catalog_u;
