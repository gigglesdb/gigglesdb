-- case: pgd_comment_sequence
-- title: comment sequence
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE SEQUENCE r0c_pgd_comment_sequence;
COMMENT ON SEQUENCE r0c_pgd_comment_sequence IS 'migration note';
SELECT obj_description('r0c_pgd_comment_sequence'::regclass,'pg_class');
COMMENT ON SEQUENCE r0c_pgd_comment_sequence IS NULL;
SELECT obj_description('r0c_pgd_comment_sequence'::regclass,'pg_class') IS NULL AS cleared;
DROP SEQUENCE r0c_pgd_comment_sequence;
