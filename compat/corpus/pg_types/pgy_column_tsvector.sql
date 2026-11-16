-- case: pgy_column_tsvector
-- title: column tsvector
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_tsvector (id int PRIMARY KEY, v tsvector);
INSERT INTO r0c_pgy_column_tsvector VALUES (1, 'cat:1 dog:2'), (2, 'cat:1 dog:2');
SELECT id, v, v=v, v @@ 'cat'::tsquery FROM r0c_pgy_column_tsvector ORDER BY v, id;
