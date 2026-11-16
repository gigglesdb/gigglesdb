-- case: pgy_column_macaddr8
-- title: column macaddr8
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_macaddr8 (id int PRIMARY KEY, v macaddr8);
INSERT INTO r0c_pgy_column_macaddr8 VALUES (1, '08:00:2b:01:02:03:04:05'), (2, '08:00:2b:01:02:03:04:05');
SELECT id, v, v=v, trunc(v) FROM r0c_pgy_column_macaddr8 ORDER BY v, id;
