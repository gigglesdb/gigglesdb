-- case: pgy_column_regclass
-- title: column regclass
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_regclass (id int PRIMARY KEY, v regclass);
INSERT INTO r0c_pgy_column_regclass VALUES (1, 'pg_catalog.pg_class'), (2, 'pg_catalog.pg_class');
SELECT id, v, v=v, v = 'pg_catalog.pg_class'::regclass FROM r0c_pgy_column_regclass ORDER BY v::text, id;
