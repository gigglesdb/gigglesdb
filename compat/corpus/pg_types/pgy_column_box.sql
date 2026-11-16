-- case: pgy_column_box
-- title: column box
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_box (id int PRIMARY KEY, v box);
INSERT INTO r0c_pgy_column_box VALUES (1, '(2,2),(0,0)'), (2, '(2,2),(0,0)');
SELECT id, v, v ~= v, area(v) FROM r0c_pgy_column_box ORDER BY v::text, id;
