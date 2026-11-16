-- case: pgp_sql_strict
-- title: SQL function STRICT
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_strict(x integer) RETURNS integer LANGUAGE sql STRICT AS $f$SELECT coalesce(x,99)$f$;
SELECT r0c_pgp_sql_strict(NULL::integer);
SELECT provolatile, proisstrict FROM pg_proc WHERE oid='r0c_pgp_sql_strict(integer)'::regprocedure;
DROP FUNCTION r0c_pgp_sql_strict(x integer);
