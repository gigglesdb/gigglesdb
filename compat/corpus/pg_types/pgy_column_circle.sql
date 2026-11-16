-- case: pgy_column_circle
-- title: column circle
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_circle (id int PRIMARY KEY, v circle);
INSERT INTO r0c_pgy_column_circle VALUES (1, '<(0,0),2>'), (2, '<(0,0),2>');
SELECT id, v, v ~= v, area(v) FROM r0c_pgy_column_circle ORDER BY v::text, id;
