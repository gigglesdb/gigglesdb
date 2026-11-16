-- case: pgp_ext_ltree
-- title: Extension ltree deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "ltree";
SELECT 'Top.Science.Astronomy'::ltree ~ 'Top.*'::lquery;
SELECT nlevel('Top.Science.Astronomy'::ltree);
DROP EXTENSION "ltree";
