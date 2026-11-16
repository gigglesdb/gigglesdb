-- case: pgp_ext_fuzzy
-- title: Extension fuzzystrmatch deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "fuzzystrmatch";
SELECT levenshtein('kitten','sitting');
DROP EXTENSION "fuzzystrmatch";
