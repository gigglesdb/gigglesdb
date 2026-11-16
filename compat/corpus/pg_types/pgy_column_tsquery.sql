-- case: pgy_column_tsquery
-- title: column tsquery
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_tsquery (id int PRIMARY KEY, v tsquery);
INSERT INTO r0c_pgy_column_tsquery VALUES (1, 'cat & dog'), (2, 'cat & dog');
SELECT id, v, v=v, v @> 'cat'::tsquery FROM r0c_pgy_column_tsquery ORDER BY v, id;
