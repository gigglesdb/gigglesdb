-- case: pgy_column_oid
-- title: column oid
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_oid (id int PRIMARY KEY, v oid);
INSERT INTO r0c_pgy_column_oid VALUES (1, 42), (2, 42);
SELECT id, v, v=v, v > 1::oid FROM r0c_pgy_column_oid ORDER BY v, id;
