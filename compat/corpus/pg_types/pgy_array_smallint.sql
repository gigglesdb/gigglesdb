-- case: pgy_array_smallint
-- title: array smallint
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_smallint (id int PRIMARY KEY, v smallint[]);
INSERT INTO r0c_pgy_array_smallint VALUES (1, '{1,2}'), (2, '{1,2}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_smallint ORDER BY v, id;
