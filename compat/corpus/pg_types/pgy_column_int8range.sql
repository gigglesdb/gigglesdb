-- case: pgy_column_int8range
-- title: column int8range
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_int8range (id int PRIMARY KEY, v int8range);
INSERT INTO r0c_pgy_column_int8range VALUES (1, '[1,5)'), (2, '[1,5)');
SELECT id, v, v = v, v @> 2::bigint FROM r0c_pgy_column_int8range ORDER BY v, id;
