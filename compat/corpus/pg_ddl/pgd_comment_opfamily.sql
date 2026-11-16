-- case: pgd_comment_opfamily
-- title: comment opfamily
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE OPERATOR FAMILY r0c_pgd_comment_opfamily USING btree;
COMMENT ON OPERATOR FAMILY r0c_pgd_comment_opfamily USING btree IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_opfamily WHERE opfname='r0c_pgd_comment_opfamily'),'pg_opfamily');
COMMENT ON OPERATOR FAMILY r0c_pgd_comment_opfamily USING btree IS NULL;
SELECT obj_description((SELECT oid FROM pg_opfamily WHERE opfname='r0c_pgd_comment_opfamily'),'pg_opfamily') IS NULL AS cleared;
DROP OPERATOR FAMILY r0c_pgd_comment_opfamily USING btree;
