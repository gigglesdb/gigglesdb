-- case: pgy_array_bytea
-- title: array bytea
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: arrays, bounds and ARRAY constructor; types.txt / Arrays
CREATE TABLE r0c_pgy_array_bytea (id int PRIMARY KEY, v bytea[]);
INSERT INTO r0c_pgy_array_bytea VALUES (1, '{abc,def}'), (2, '{abc,def}');
SELECT id, v, v = v, v @> v FROM r0c_pgy_array_bytea ORDER BY v, id;
