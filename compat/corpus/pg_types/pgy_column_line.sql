-- case: pgy_column_line
-- title: column line
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_line (id int PRIMARY KEY, v line);
INSERT INTO r0c_pgy_column_line VALUES (1, '{1,-1,0}'), (2, '{1,-1,0}');
SELECT id, v, v::text = v::text, v ?# line(point(0,0),point(1,1)) FROM r0c_pgy_column_line ORDER BY v::text, id;
