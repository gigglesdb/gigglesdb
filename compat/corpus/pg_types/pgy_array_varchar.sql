-- case: pgy_array_varchar
-- title: array varchar
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_varchar (id int PRIMARY KEY, v varchar[]);
INSERT INTO r0c_pgy_array_varchar VALUES (1, '{a,b}'), (2, '{a,b}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_varchar ORDER BY v, id;
