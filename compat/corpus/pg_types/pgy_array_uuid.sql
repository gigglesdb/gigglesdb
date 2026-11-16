-- case: pgy_array_uuid
-- title: array uuid
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_uuid (id int PRIMARY KEY, v uuid[]);
INSERT INTO r0c_pgy_array_uuid VALUES (1, '{123e4567-e89b-12d3-a456-426614174000}'), (2, '{123e4567-e89b-12d3-a456-426614174000}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_uuid ORDER BY v, id;
