-- case: pgy_column_int8multirange
-- title: column int8multirange
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_int8multirange (id int PRIMARY KEY, v int8multirange);
INSERT INTO r0c_pgy_column_int8multirange VALUES (1, '{[1,5)}'), (2, '{[1,5)}');
SELECT id, v, v = v, v @> 2::bigint FROM r0c_pgy_column_int8multirange ORDER BY v, id;
