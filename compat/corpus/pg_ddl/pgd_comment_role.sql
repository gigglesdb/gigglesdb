-- case: pgd_comment_role
-- title: comment role
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE ROLE r0c_pgd_comment_role;
COMMENT ON ROLE r0c_pgd_comment_role IS 'migration note';
SELECT shobj_description((SELECT oid FROM pg_roles WHERE rolname='r0c_pgd_comment_role'),'pg_authid');
COMMENT ON ROLE r0c_pgd_comment_role IS NULL;
SELECT shobj_description((SELECT oid FROM pg_roles WHERE rolname='r0c_pgd_comment_role'),'pg_authid') IS NULL AS cleared;
DROP ROLE r0c_pgd_comment_role;
