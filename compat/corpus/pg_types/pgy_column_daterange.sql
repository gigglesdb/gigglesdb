-- case: pgy_column_daterange
-- title: column daterange
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_daterange (id int PRIMARY KEY, v daterange);
INSERT INTO r0c_pgy_column_daterange VALUES (1, '[2024-01-01,2024-02-01)'), (2, '[2024-01-01,2024-02-01)');
SELECT id, v, v = v, v @> '2024-01-02'::date FROM r0c_pgy_column_daterange ORDER BY v, id;
