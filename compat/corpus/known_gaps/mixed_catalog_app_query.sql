-- case: mixed_catalog_app_query
-- title: Mixed catalog app query
-- owner: G
-- expect: compatible
CREATE TABLE r0c_mixed_catalog_app_query_t (id int);
SELECT c.relname, count(t.*) FROM pg_catalog.pg_class c LEFT JOIN r0c_mixed_catalog_app_query_t t ON true WHERE c.relname = 'r0c_mixed_catalog_app_query_t' GROUP BY c.relname ORDER BY c.relname;
