-- case: pgd_comment_statistics
-- title: comment statistics
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE TABLE r0c_pgd_comment_statistics (id int PRIMARY KEY, a int,b int);
CREATE STATISTICS r0c_pgd_comment_statistics_s ON a,b FROM r0c_pgd_comment_statistics;
COMMENT ON STATISTICS r0c_pgd_comment_statistics_s IS 'migration note';
SELECT obj_description((SELECT oid FROM pg_statistic_ext WHERE stxname='r0c_pgd_comment_statistics_s'),'pg_statistic_ext');
COMMENT ON STATISTICS r0c_pgd_comment_statistics_s IS NULL;
SELECT obj_description((SELECT oid FROM pg_statistic_ext WHERE stxname='r0c_pgd_comment_statistics_s'),'pg_statistic_ext') IS NULL AS cleared;
DROP STATISTICS r0c_pgd_comment_statistics_s;
