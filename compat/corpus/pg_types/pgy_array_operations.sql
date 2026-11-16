-- case: pgy_array_operations
-- title: array operations
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_operations (id int PRIMARY KEY, v int[], tags text[]);
INSERT INTO r0c_pgy_array_operations VALUES (1,'{1,2,NULL}','{a,b}');
SELECT 2 = ANY(v), cardinality(v), v @> ARRAY[2], tags || '{c}', tags || '{c}'::text[], array_position(v,2) FROM r0c_pgy_array_operations ORDER BY id;
SELECT x FROM r0c_pgy_array_operations CROSS JOIN LATERAL unnest(v) AS x ORDER BY x NULLS LAST;
SELECT array_agg(id ORDER BY id) FROM r0c_pgy_array_operations;
-- @error
SELECT tags || 'c' FROM r0c_pgy_array_operations;
