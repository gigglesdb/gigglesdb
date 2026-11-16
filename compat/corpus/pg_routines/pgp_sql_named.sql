-- case: pgp_sql_named
-- title: Named SQL argument notation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_named(x integer, y integer) RETURNS integer LANGUAGE sql AS $f$SELECT x-y$f$;
SELECT r0c_pgp_sql_named(y => 2, x => 9);
DROP FUNCTION r0c_pgp_sql_named(x integer, y integer);
