-- case: pgd_comment_ts_template
-- title: comment ts template
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE TEXT SEARCH TEMPLATE r0c_pgd_comment_ts_template (INIT=dsimple_init,LEXIZE=dsimple_lexize);
COMMENT ON TEXT SEARCH TEMPLATE r0c_pgd_comment_ts_template IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_ts_template WHERE tmplname='r0c_pgd_comment_ts_template'),'pg_ts_template');
COMMENT ON TEXT SEARCH TEMPLATE r0c_pgd_comment_ts_template IS NULL;
SELECT obj_description((SELECT oid FROM pg_ts_template WHERE tmplname='r0c_pgd_comment_ts_template'),'pg_ts_template') IS NULL AS cleared;
DROP TEXT SEARCH TEMPLATE r0c_pgd_comment_ts_template;
