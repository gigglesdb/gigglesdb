-- case: pgp_function_alter
-- title: ALTER FUNCTION rename owner settings cost volatility
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_function_alter(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;
ALTER FUNCTION r0c_pgp_function_alter(integer) RENAME TO r0c_pgp_function_alter_renamed;
ALTER FUNCTION r0c_pgp_function_alter_renamed(integer) OWNER TO postgres;
ALTER FUNCTION r0c_pgp_function_alter_renamed(integer) SET search_path TO pg_catalog;
ALTER FUNCTION r0c_pgp_function_alter_renamed(integer) COST 25;
ALTER FUNCTION r0c_pgp_function_alter_renamed(integer) IMMUTABLE;
SELECT provolatile, procost, proconfig, pg_get_userbyid(proowner) FROM pg_proc WHERE oid='r0c_pgp_function_alter_renamed(integer)'::regprocedure;
DROP FUNCTION r0c_pgp_function_alter_renamed(integer);
