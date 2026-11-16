-- case: pgp_sql_set
-- title: SQL set returning function
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_set(n integer) RETURNS SETOF integer LANGUAGE sql AS $f$SELECT generate_series(1,n)$f$;
SELECT * FROM r0c_pgp_sql_set(3) ORDER BY 1;
DROP FUNCTION r0c_pgp_sql_set(n integer);
