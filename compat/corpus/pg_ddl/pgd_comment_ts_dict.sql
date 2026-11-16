-- case: pgd_comment_ts_dict
-- title: comment ts dict
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE TEXT SEARCH DICTIONARY r0c_pgd_comment_ts_dict (TEMPLATE=pg_catalog.simple);
COMMENT ON TEXT SEARCH DICTIONARY r0c_pgd_comment_ts_dict IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_ts_dict WHERE dictname='r0c_pgd_comment_ts_dict'),'pg_ts_dict');
COMMENT ON TEXT SEARCH DICTIONARY r0c_pgd_comment_ts_dict IS NULL;
SELECT obj_description((SELECT oid FROM pg_ts_dict WHERE dictname='r0c_pgd_comment_ts_dict'),'pg_ts_dict') IS NULL AS cleared;
DROP TEXT SEARCH DICTIONARY r0c_pgd_comment_ts_dict;
