-- case: pgy_column_path
-- title: column path
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_path (id int PRIMARY KEY, v path);
INSERT INTO r0c_pgy_column_path VALUES (1, '[(0,0),(1,1)]'), (2, '[(0,0),(1,1)]');
SELECT id, v, v::text = v::text, length(v) FROM r0c_pgy_column_path ORDER BY v::text, id;
