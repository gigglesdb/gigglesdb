-- case: pgd_comment_table
-- title: comment table
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_table (id int PRIMARY KEY, v int);
COMMENT ON TABLE r0c_pgd_comment_table IS 'migration note';
SELECT obj_description('r0c_pgd_comment_table'::regclass,'pg_class');
COMMENT ON TABLE r0c_pgd_comment_table IS NULL;
SELECT obj_description('r0c_pgd_comment_table'::regclass,'pg_class') IS NULL AS cleared;
