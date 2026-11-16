-- case: pgd_comment_collation
-- title: comment collation
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE COLLATION r0c_pgd_comment_collation FROM "C";
COMMENT ON COLLATION r0c_pgd_comment_collation IS 'migration note';
SELECT obj_description((SELECT oid FROM pg_collation WHERE collname='r0c_pgd_comment_collation'),'pg_collation');
COMMENT ON COLLATION r0c_pgd_comment_collation IS NULL;
SELECT obj_description((SELECT oid FROM pg_collation WHERE collname='r0c_pgd_comment_collation'),'pg_collation') IS NULL AS cleared;
DROP COLLATION r0c_pgd_comment_collation;
