-- case: pgf_array_dimensions
-- title: Array bounds dimensions containment and ANY
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-array.html
CREATE TABLE r0c_pgf_array_dimensions (id integer PRIMARY KEY, a integer[]);
INSERT INTO r0c_pgf_array_dimensions VALUES (1,'{1,2,3}'),(2,'[0:1]={4,5}'),(3,NULL);
SELECT id, array_length(a,1) FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, array_lower(a,1) FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, array_upper(a,1) FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, array_dims(a) FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, array_ndims(a) FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, cardinality(a) FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, a @> ARRAY[2] FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, a <@ ARRAY[1,2,3,4,5] FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, a && ARRAY[2,5] FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, 2 = ANY(a) FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, a[1] FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, a[1:2] FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, ARRAY[[1,2],[3,4]] FROM r0c_pgf_array_dimensions ORDER BY id;
SELECT id, '{1,2}'::integer[] || '{3}' FROM r0c_pgf_array_dimensions ORDER BY id;
DROP TABLE r0c_pgf_array_dimensions;
