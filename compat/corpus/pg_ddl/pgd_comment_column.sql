-- case: pgd_comment_column
-- title: comment column
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_column (id int PRIMARY KEY, v int);
COMMENT ON COLUMN r0c_pgd_comment_column.v IS 'migration note';
SELECT col_description('r0c_pgd_comment_column'::regclass,2);
COMMENT ON COLUMN r0c_pgd_comment_column.v IS NULL;
SELECT col_description('r0c_pgd_comment_column'::regclass,2) IS NULL AS cleared;
