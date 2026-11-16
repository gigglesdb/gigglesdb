-- case: pgp_ext_statements
-- title: pg_stat_statements requires shared preload
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
CREATE EXTENSION pg_stat_statements;
-- @error
SELECT count(*) FROM pg_stat_statements;
DROP EXTENSION pg_stat_statements;
