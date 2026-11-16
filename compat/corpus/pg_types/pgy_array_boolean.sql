-- case: pgy_array_boolean
-- title: array boolean
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_boolean (id int PRIMARY KEY, v boolean[]);
INSERT INTO r0c_pgy_array_boolean VALUES (1, '{true,false,NULL}'), (2, '{true,false,NULL}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_boolean ORDER BY v, id;
