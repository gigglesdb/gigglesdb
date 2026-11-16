-- case: pgy_column_nummultirange
-- title: column nummultirange
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_nummultirange (id int PRIMARY KEY, v nummultirange);
INSERT INTO r0c_pgy_column_nummultirange VALUES (1, '{[1,5)}'), (2, '{[1,5)}');
SELECT id, v, v = v, v @> 2::numeric FROM r0c_pgy_column_nummultirange ORDER BY v, id;
