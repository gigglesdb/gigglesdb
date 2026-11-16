-- case: pgy_column_tstzmultirange
-- title: column tstzmultirange
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_tstzmultirange (id int PRIMARY KEY, v tstzmultirange);
INSERT INTO r0c_pgy_column_tstzmultirange VALUES (1, '{[2024-01-01+00,2024-02-01+00)}'), (2, '{[2024-01-01+00,2024-02-01+00)}');
SELECT id, v, v = v, v @> '2024-01-02+00'::timestamptz FROM r0c_pgy_column_tstzmultirange ORDER BY v, id;
