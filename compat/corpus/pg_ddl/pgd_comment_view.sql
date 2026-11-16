-- case: pgd_comment_view
-- title: comment view
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_view (id int PRIMARY KEY, v int);
CREATE VIEW r0c_pgd_comment_view_v AS SELECT * FROM r0c_pgd_comment_view;
COMMENT ON VIEW r0c_pgd_comment_view_v IS 'migration note';
SELECT obj_description('r0c_pgd_comment_view_v'::regclass,'pg_class');
COMMENT ON VIEW r0c_pgd_comment_view_v IS NULL;
SELECT obj_description('r0c_pgd_comment_view_v'::regclass,'pg_class') IS NULL AS cleared;
DROP VIEW r0c_pgd_comment_view_v;
