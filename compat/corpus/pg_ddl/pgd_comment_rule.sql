-- case: pgd_comment_rule
-- title: comment rule
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_rule (id int PRIMARY KEY, v int);
CREATE RULE r0c_pgd_comment_rule_r AS ON INSERT TO r0c_pgd_comment_rule DO ALSO NOTHING;
COMMENT ON RULE r0c_pgd_comment_rule_r ON r0c_pgd_comment_rule IS 'migration note';
SELECT obj_description((SELECT oid FROM pg_rewrite WHERE rulename='r0c_pgd_comment_rule_r'),'pg_rewrite');
COMMENT ON RULE r0c_pgd_comment_rule_r ON r0c_pgd_comment_rule IS NULL;
SELECT obj_description((SELECT oid FROM pg_rewrite WHERE rulename='r0c_pgd_comment_rule_r'),'pg_rewrite') IS NULL AS cleared;
DROP RULE r0c_pgd_comment_rule_r ON r0c_pgd_comment_rule;
