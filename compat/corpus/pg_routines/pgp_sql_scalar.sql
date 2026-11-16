-- case: pgp_sql_scalar
-- title: SQL scalar application calculation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_scalar(x integer) RETURNS integer LANGUAGE sql AS $f$SELECT x * 2$f$;
SELECT r0c_pgp_sql_scalar(21);
DROP FUNCTION r0c_pgp_sql_scalar(x integer);
