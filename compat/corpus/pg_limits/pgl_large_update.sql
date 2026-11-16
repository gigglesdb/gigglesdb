-- case: pgl_large_update
-- title: Fifty thousand row UPDATE completes and changes every row
-- owner: E1
-- expect: compatible
-- source: Maintenance: large UPDATE correctness
CREATE TABLE r0c_pgl_large_update (id integer PRIMARY KEY, value integer);
INSERT INTO r0c_pgl_large_update SELECT g, 0 FROM generate_series(1, 50000) AS g;
UPDATE r0c_pgl_large_update SET value = id + 1;
SELECT count(*), min(value), max(value), count(*) FILTER (WHERE value = id + 1) AS changed FROM r0c_pgl_large_update;
DROP TABLE r0c_pgl_large_update;
