-- case: pgp_function_catalog
-- title: Deterministic backslash df style pg_proc listing
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_function_catalog(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$;
CREATE FUNCTION r0c_pgp_function_catalog(text) RETURNS text LANGUAGE sql AS $$SELECT $1$$;
SELECT n.nspname,p.proname,pg_get_function_identity_arguments(p.oid) AS args,pg_get_function_result(p.oid) AS result,p.prokind FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.proname='r0c_pgp_function_catalog' ORDER BY n.nspname,p.proname,args;
DROP FUNCTION r0c_pgp_function_catalog(integer);
DROP FUNCTION r0c_pgp_function_catalog(text);
