-- case: pgy_column_numrange
-- title: column numrange
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_numrange (id int PRIMARY KEY, v numrange);
INSERT INTO r0c_pgy_column_numrange VALUES (1, '[1,5)'), (2, '[1,5)');
SELECT id, v, v = v, v @> 2::numeric FROM r0c_pgy_column_numrange ORDER BY v, id;
