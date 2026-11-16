-- case: pgp_ext_intarray
-- title: Extension intarray deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "intarray";
SELECT sort(ARRAY[3,1,2]);
SELECT ARRAY[1,2,3] & ARRAY[2,4];
DROP EXTENSION "intarray";
