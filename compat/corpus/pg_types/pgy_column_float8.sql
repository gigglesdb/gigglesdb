-- case: pgy_column_float8
-- title: column float8
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_float8 (id int PRIMARY KEY, v float8);
INSERT INTO r0c_pgy_column_float8 VALUES (1, 7), (2, 7);
SELECT id, v, v = v, v + 1 FROM r0c_pgy_column_float8 ORDER BY v, id;
