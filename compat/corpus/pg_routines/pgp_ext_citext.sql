-- case: pgp_ext_citext
-- title: Extension citext deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "citext";
SELECT 'Hello'::citext='hello'::citext;
DROP EXTENSION "citext";
