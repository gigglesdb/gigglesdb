-- case: pgd_comment_constraint
-- title: comment constraint
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_constraint (id int PRIMARY KEY, v int);
ALTER TABLE r0c_pgd_comment_constraint ADD CONSTRAINT r0c_pgd_comment_constraint_ck CHECK(v>0);
COMMENT ON CONSTRAINT r0c_pgd_comment_constraint_ck ON r0c_pgd_comment_constraint IS 'migration note';
SELECT obj_description((SELECT oid FROM pg_constraint WHERE conname='r0c_pgd_comment_constraint_ck'),'pg_constraint');
COMMENT ON CONSTRAINT r0c_pgd_comment_constraint_ck ON r0c_pgd_comment_constraint IS NULL;
SELECT obj_description((SELECT oid FROM pg_constraint WHERE conname='r0c_pgd_comment_constraint_ck'),'pg_constraint') IS NULL AS cleared;
