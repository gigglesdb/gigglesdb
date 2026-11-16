-- case: pgp_ext_unaccent
-- title: Extension unaccent deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "unaccent";
SELECT unaccent('H?tel');
DROP EXTENSION "unaccent";
