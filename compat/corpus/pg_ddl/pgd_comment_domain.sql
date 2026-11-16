-- case: pgd_comment_domain
-- title: comment domain
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE DOMAIN r0c_pgd_comment_domain AS int CHECK(VALUE>0);
COMMENT ON DOMAIN r0c_pgd_comment_domain IS 'migration note';
SELECT obj_description('r0c_pgd_comment_domain'::regtype,'pg_type');
COMMENT ON DOMAIN r0c_pgd_comment_domain IS NULL;
SELECT obj_description('r0c_pgd_comment_domain'::regtype,'pg_type') IS NULL AS cleared;
DROP DOMAIN r0c_pgd_comment_domain;
