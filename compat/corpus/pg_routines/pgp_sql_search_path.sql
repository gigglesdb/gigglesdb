-- case: pgp_sql_search_path
-- title: Function local search_path
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 stored logic
CREATE FUNCTION r0c_pgp_sql_search_path() RETURNS text LANGUAGE sql SET search_path = pg_catalog AS $f$SELECT current_setting('search_path')$f$;
SELECT r0c_pgp_sql_search_path();
DROP FUNCTION r0c_pgp_sql_search_path();
