-- case: pgp_ext_trgm
-- title: Extension pg_trgm deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "pg_trgm";
SELECT round(similarity('hello','hallo')::numeric,4);
DROP EXTENSION "pg_trgm";
