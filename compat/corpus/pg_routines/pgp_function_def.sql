-- case: pgp_function_def
-- title: pg_get_functiondef reconstructs SQL body
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_function_def(integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT $1 + 1$$;
SELECT pg_get_functiondef('r0c_pgp_function_def(integer)'::regprocedure);
DROP FUNCTION r0c_pgp_function_def(integer);
