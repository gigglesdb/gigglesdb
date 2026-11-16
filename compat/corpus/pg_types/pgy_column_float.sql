-- case: pgy_column_float
-- title: column float
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_float (id int PRIMARY KEY, v float);
INSERT INTO r0c_pgy_column_float VALUES (1, 7), (2, 7);
SELECT id, v, v = v, v + 1 FROM r0c_pgy_column_float ORDER BY v, id;
