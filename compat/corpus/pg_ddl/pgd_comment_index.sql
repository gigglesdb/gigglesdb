-- case: pgd_comment_index
-- title: comment index
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_index (id int PRIMARY KEY, v int);
CREATE INDEX r0c_pgd_comment_index_i ON r0c_pgd_comment_index(v);
COMMENT ON INDEX r0c_pgd_comment_index_i IS 'migration note';
SELECT obj_description('r0c_pgd_comment_index_i'::regclass,'pg_class');
COMMENT ON INDEX r0c_pgd_comment_index_i IS NULL;
SELECT obj_description('r0c_pgd_comment_index_i'::regclass,'pg_class') IS NULL AS cleared;
DROP INDEX r0c_pgd_comment_index_i;
