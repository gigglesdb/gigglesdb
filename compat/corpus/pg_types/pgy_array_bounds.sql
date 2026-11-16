-- case: pgy_array_bounds
-- title: array bounds
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_bounds (id int PRIMARY KEY, v int[]);
INSERT INTO r0c_pgy_array_bounds VALUES (1,'[0:1]={1,2}');
SELECT v, v[0], v[1], array_lower(v,1), array_upper(v,1), v = ARRAY[1,2] FROM r0c_pgy_array_bounds ORDER BY id;
