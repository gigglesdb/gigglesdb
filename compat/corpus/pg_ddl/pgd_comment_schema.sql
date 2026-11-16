-- case: pgd_comment_schema
-- title: comment schema
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE SCHEMA r0c_pgd_comment_schema;
COMMENT ON SCHEMA r0c_pgd_comment_schema IS 'migration note';
SELECT obj_description((SELECT oid FROM pg_namespace WHERE nspname='r0c_pgd_comment_schema'),'pg_namespace');
COMMENT ON SCHEMA r0c_pgd_comment_schema IS NULL;
SELECT obj_description((SELECT oid FROM pg_namespace WHERE nspname='r0c_pgd_comment_schema'),'pg_namespace') IS NULL AS cleared;
DROP SCHEMA r0c_pgd_comment_schema;
