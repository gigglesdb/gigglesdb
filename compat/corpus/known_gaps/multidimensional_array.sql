-- case: multidimensional_array
-- title: Arrays with more than one dimension
-- owner: E2
-- expect: compatible
-- source: Differences: Published differences / multidimensional arrays
CREATE TABLE r0c_multidimensional_array (id int PRIMARY KEY, v int[]);
INSERT INTO r0c_multidimensional_array VALUES (1, '{{1,2},{3,4}}');
SELECT id, v[2][1], array_dims(v), array_ndims(v) FROM r0c_multidimensional_array WHERE id = 1;
INSERT INTO r0c_multidimensional_array VALUES (2, ARRAY[[1,2],[3,4]]);
SELECT id, v[2][1], array_dims(v), array_ndims(v) FROM r0c_multidimensional_array ORDER BY id;
