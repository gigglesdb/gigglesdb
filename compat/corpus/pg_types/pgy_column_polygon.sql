-- case: pgy_column_polygon
-- title: column polygon
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_polygon (id int PRIMARY KEY, v polygon);
INSERT INTO r0c_pgy_column_polygon VALUES (1, '((0,0),(2,0),(0,2))'), (2, '((0,0),(2,0),(0,2))');
SELECT id, v, v ~= v, npoints(v) FROM r0c_pgy_column_polygon ORDER BY v::text, id;
