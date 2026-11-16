-- case: pgy_column_int2
-- title: column int2
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_int2 (id int PRIMARY KEY, v int2);
INSERT INTO r0c_pgy_column_int2 VALUES (1, 7), (2, 7);
SELECT id, v, v = v, v + 1 FROM r0c_pgy_column_int2 ORDER BY v, id;
