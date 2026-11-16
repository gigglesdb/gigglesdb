-- case: pgy_column_name
-- title: column name
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_name (id int PRIMARY KEY, v name);
INSERT INTO r0c_pgy_column_name VALUES (1, 'hello'), (2, 'hello');
SELECT id, v, v=v, length(v::text) FROM r0c_pgy_column_name ORDER BY v, id;
