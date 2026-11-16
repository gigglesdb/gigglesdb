-- case: pgr_all_functions
-- title: Bulk grants on all functions in a schema
-- owner: unowned
-- expect: compatible
-- source: Roles: GRANT ON ALL TABLES SEQUENCES FUNCTIONS IN SCHEMA
CREATE ROLE r0c_pgr_all_functions_u;
CREATE SCHEMA r0c_pgr_all_functions;
GRANT USAGE ON SCHEMA r0c_pgr_all_functions TO r0c_pgr_all_functions_u;
CREATE FUNCTION r0c_pgr_all_functions.r0c_pgr_all_functions_f(integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$ SELECT $1 + 1 $$;
REVOKE EXECUTE ON FUNCTION r0c_pgr_all_functions.r0c_pgr_all_functions_f(integer) FROM PUBLIC;
SET ROLE r0c_pgr_all_functions_u;
-- @error
SELECT r0c_pgr_all_functions.r0c_pgr_all_functions_f(4);
RESET ROLE;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA r0c_pgr_all_functions TO r0c_pgr_all_functions_u;
SET ROLE r0c_pgr_all_functions_u;
SELECT r0c_pgr_all_functions.r0c_pgr_all_functions_f(4);
RESET ROLE;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA r0c_pgr_all_functions FROM r0c_pgr_all_functions_u;
DROP FUNCTION r0c_pgr_all_functions.r0c_pgr_all_functions_f(integer);
DROP SCHEMA r0c_pgr_all_functions;
DROP ROLE r0c_pgr_all_functions_u;
