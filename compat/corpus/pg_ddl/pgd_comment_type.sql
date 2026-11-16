-- case: pgd_comment_type
-- title: comment type
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TYPE r0c_pgd_comment_type AS ENUM('a','b');
COMMENT ON TYPE r0c_pgd_comment_type IS 'migration note';
SELECT obj_description('r0c_pgd_comment_type'::regtype,'pg_type');
COMMENT ON TYPE r0c_pgd_comment_type IS NULL;
SELECT obj_description('r0c_pgd_comment_type'::regtype,'pg_type') IS NULL AS cleared;
DROP TYPE r0c_pgd_comment_type;
