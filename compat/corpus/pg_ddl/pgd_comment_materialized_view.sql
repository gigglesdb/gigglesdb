-- case: pgd_comment_materialized_view
-- title: comment materialized view
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_materialized_view (id int PRIMARY KEY, v int);
CREATE MATERIALIZED VIEW r0c_pgd_comment_materialized_view_v AS SELECT * FROM r0c_pgd_comment_materialized_view;
COMMENT ON MATERIALIZED VIEW r0c_pgd_comment_materialized_view_v IS 'migration note';
SELECT obj_description('r0c_pgd_comment_materialized_view_v'::regclass,'pg_class');
COMMENT ON MATERIALIZED VIEW r0c_pgd_comment_materialized_view_v IS NULL;
SELECT obj_description('r0c_pgd_comment_materialized_view_v'::regclass,'pg_class') IS NULL AS cleared;
DROP MATERIALIZED VIEW r0c_pgd_comment_materialized_view_v;
