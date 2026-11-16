-- case: pgy_array_timestamptz
-- title: array timestamptz
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_timestamptz (id int PRIMARY KEY, v timestamptz[]);
INSERT INTO r0c_pgy_array_timestamptz VALUES (1, '{2024-01-02T00:00:00Z}'), (2, '{2024-01-02T00:00:00Z}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_timestamptz ORDER BY v, id;
