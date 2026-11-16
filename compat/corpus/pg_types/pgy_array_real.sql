-- case: pgy_array_real
-- title: array real
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_real (id int PRIMARY KEY, v real[]);
INSERT INTO r0c_pgy_array_real VALUES (1, '{1,2}'), (2, '{1,2}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_real ORDER BY v, id;
