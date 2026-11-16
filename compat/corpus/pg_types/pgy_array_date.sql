-- case: pgy_array_date
-- title: array date
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_date (id int PRIMARY KEY, v date[]);
INSERT INTO r0c_pgy_array_date VALUES (1, '{2024-01-02,2024-01-03}'), (2, '{2024-01-02,2024-01-03}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_date ORDER BY v, id;
