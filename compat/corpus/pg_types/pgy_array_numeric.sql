-- case: pgy_array_numeric
-- title: array numeric
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_numeric (id int PRIMARY KEY, v numeric[]);
INSERT INTO r0c_pgy_array_numeric VALUES (1, '{1.2,NULL,3}'), (2, '{1.2,NULL,3}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_numeric ORDER BY v, id;
