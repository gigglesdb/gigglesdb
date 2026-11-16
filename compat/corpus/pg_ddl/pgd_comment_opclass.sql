-- case: pgd_comment_opclass
-- title: comment opclass
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE OPERATOR CLASS r0c_pgd_comment_opclass FOR TYPE int USING btree AS OPERATOR 1 < (int,int),OPERATOR 2 <= (int,int),OPERATOR 3 = (int,int),OPERATOR 4 >= (int,int),OPERATOR 5 > (int,int),FUNCTION 1 btint4cmp(int,int);
COMMENT ON OPERATOR CLASS r0c_pgd_comment_opclass USING btree IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_opclass WHERE opcname='r0c_pgd_comment_opclass'),'pg_opclass');
COMMENT ON OPERATOR CLASS r0c_pgd_comment_opclass USING btree IS NULL;
SELECT obj_description((SELECT oid FROM pg_opclass WHERE opcname='r0c_pgd_comment_opclass'),'pg_opclass') IS NULL AS cleared;
DROP OPERATOR CLASS r0c_pgd_comment_opclass USING btree;
DROP OPERATOR FAMILY r0c_pgd_comment_opclass USING btree;
