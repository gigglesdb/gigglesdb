-- case: pgd_comment_ts_config
-- title: comment ts config
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE TEXT SEARCH CONFIGURATION r0c_pgd_comment_ts_config (COPY=pg_catalog.simple);
COMMENT ON TEXT SEARCH CONFIGURATION r0c_pgd_comment_ts_config IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_ts_config WHERE cfgname='r0c_pgd_comment_ts_config'),'pg_ts_config');
COMMENT ON TEXT SEARCH CONFIGURATION r0c_pgd_comment_ts_config IS NULL;
SELECT obj_description((SELECT oid FROM pg_ts_config WHERE cfgname='r0c_pgd_comment_ts_config'),'pg_ts_config') IS NULL AS cleared;
DROP TEXT SEARCH CONFIGURATION r0c_pgd_comment_ts_config;
