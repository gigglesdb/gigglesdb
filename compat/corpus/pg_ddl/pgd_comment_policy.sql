-- case: pgd_comment_policy
-- title: comment policy
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_policy (id int PRIMARY KEY, v int);
CREATE POLICY r0c_pgd_comment_policy_p ON r0c_pgd_comment_policy USING (true);
COMMENT ON POLICY r0c_pgd_comment_policy_p ON r0c_pgd_comment_policy IS 'migration note';
SELECT obj_description((SELECT oid FROM pg_policy WHERE polname='r0c_pgd_comment_policy_p'),'pg_policy');
COMMENT ON POLICY r0c_pgd_comment_policy_p ON r0c_pgd_comment_policy IS NULL;
SELECT obj_description((SELECT oid FROM pg_policy WHERE polname='r0c_pgd_comment_policy_p'),'pg_policy') IS NULL AS cleared;
DROP POLICY r0c_pgd_comment_policy_p ON r0c_pgd_comment_policy;
