-- case: pgy_array_jsonb
-- title: array jsonb
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_jsonb (id int PRIMARY KEY, v jsonb[]);
INSERT INTO r0c_pgy_array_jsonb VALUES (1, '{1,2}'), (2, '{1,2}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_jsonb ORDER BY v, id;
