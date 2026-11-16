-- case: pgp_sql_stable
-- title: SQL function STABLE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_stable(x integer) RETURNS integer LANGUAGE sql STABLE AS $f$SELECT coalesce(x,99)$f$;
SELECT r0c_pgp_sql_stable(NULL::integer);
SELECT provolatile, proisstrict FROM pg_proc WHERE oid='r0c_pgp_sql_stable(integer)'::regprocedure;
DROP FUNCTION r0c_pgp_sql_stable(x integer);
