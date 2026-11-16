-- case: pgp_sql_defaults
-- title: Defaulted SQL function argument
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_defaults(x integer, y integer DEFAULT 10) RETURNS integer LANGUAGE sql AS $f$SELECT x+y$f$;
SELECT r0c_pgp_sql_defaults(2);
DROP FUNCTION r0c_pgp_sql_defaults(integer,integer);
