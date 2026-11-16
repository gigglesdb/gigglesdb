-- case: pgy_array_multidimensional
-- title: array multidimensional
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_multidimensional (id int PRIMARY KEY, v int[][]);
INSERT INTO r0c_pgy_array_multidimensional VALUES (1,'{{1,2},{3,4}}');
SELECT v, v[2][1], array_ndims(v), array_length(v,2), v[1:2][2:2] FROM r0c_pgy_array_multidimensional ORDER BY id;
