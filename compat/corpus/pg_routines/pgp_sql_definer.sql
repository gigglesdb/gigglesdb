-- case: pgp_sql_definer
-- title: Security definer metadata and execution
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_definer(x integer) RETURNS integer LANGUAGE sql SECURITY DEFINER AS $f$SELECT x+1$f$;
SELECT r0c_pgp_sql_definer(2);
SELECT prosecdef FROM pg_proc WHERE oid='r0c_pgp_sql_definer(integer)'::regprocedure;
DROP FUNCTION r0c_pgp_sql_definer(x integer);
