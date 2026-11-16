-- case: pgy_column_point
-- title: column point
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_point (id int PRIMARY KEY, v point);
INSERT INTO r0c_pgy_column_point VALUES (1, '(1,2)'), (2, '(1,2)');
SELECT id, v, v ~= v, v <-> point(0,0) FROM r0c_pgy_column_point ORDER BY v::text, id;
