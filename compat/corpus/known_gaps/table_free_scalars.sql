-- case: table_free_scalars
-- title: Table free scalars
-- owner: E2
-- expect: compatible
SELECT lower('ABC') AS l, abs(-7) AS a;
