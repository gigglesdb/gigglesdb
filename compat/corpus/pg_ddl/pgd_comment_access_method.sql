-- case: pgd_comment_access_method
-- title: comment access method
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE ACCESS METHOD r0c_pgd_comment_access_method TYPE TABLE HANDLER heap_tableam_handler;
COMMENT ON ACCESS METHOD r0c_pgd_comment_access_method IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_am WHERE amname='r0c_pgd_comment_access_method'),'pg_am');
COMMENT ON ACCESS METHOD r0c_pgd_comment_access_method IS NULL;
SELECT obj_description((SELECT oid FROM pg_am WHERE amname='r0c_pgd_comment_access_method'),'pg_am') IS NULL AS cleared;
DROP ACCESS METHOD r0c_pgd_comment_access_method;
