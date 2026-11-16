-- case: pgp_sql_immutable
-- title: SQL function IMMUTABLE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_immutable(x integer) RETURNS integer LANGUAGE sql IMMUTABLE AS $f$SELECT coalesce(x,99)$f$;
SELECT r0c_pgp_sql_immutable(NULL::integer);
SELECT provolatile, proisstrict FROM pg_proc WHERE oid='r0c_pgp_sql_immutable(integer)'::regprocedure;
DROP FUNCTION r0c_pgp_sql_immutable(x integer);
