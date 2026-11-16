-- case: pgy_column_int4multirange
-- title: column int4multirange
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_int4multirange (id int PRIMARY KEY, v int4multirange);
INSERT INTO r0c_pgy_column_int4multirange VALUES (1, '{[1,5)}'), (2, '{[1,5)}');
SELECT id, v, v = v, v @> 2 FROM r0c_pgy_column_int4multirange ORDER BY v, id;
