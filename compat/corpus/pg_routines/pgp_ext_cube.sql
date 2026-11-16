-- case: pgp_ext_cube
-- title: Extension cube deterministic application operation
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 extensibility
-- Extension names are fixed by PostgreSQL control files.
CREATE EXTENSION "cube";
SELECT cube(ARRAY[1.0,2.0]) <-> cube(ARRAY[4.0,6.0]);
DROP EXTENSION "cube";
