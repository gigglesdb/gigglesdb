-- case: pgy_column_lseg
-- title: column lseg
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_lseg (id int PRIMARY KEY, v lseg);
INSERT INTO r0c_pgy_column_lseg VALUES (1, '[(0,0),(2,2)]'), (2, '[(0,0),(2,2)]');
SELECT id, v, v::text = v::text, length(v) FROM r0c_pgy_column_lseg ORDER BY v::text, id;
