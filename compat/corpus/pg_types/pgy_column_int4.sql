-- case: pgy_column_int4
-- title: column int4
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_int4 (id int PRIMARY KEY, v int4);
INSERT INTO r0c_pgy_column_int4 VALUES (1, 7), (2, 7);
SELECT id, v, v = v, v + 1 FROM r0c_pgy_column_int4 ORDER BY v, id;
