-- case: pgd_statistics
-- title: statistics
-- owner: G
-- expect: compatible
-- source: W4 CREATE ALTER DROP STATISTICS
CREATE TABLE r0c_pgd_statistics (id int PRIMARY KEY, a int,b int);
CREATE STATISTICS r0c_pgd_statistics_s (dependencies,ndistinct,mcv) ON a,b FROM r0c_pgd_statistics;
INSERT INTO r0c_pgd_statistics SELECT g,g%2,g%2 FROM generate_series(1,20) g;
ANALYZE r0c_pgd_statistics;
ALTER STATISTICS r0c_pgd_statistics_s SET STATISTICS 100;
ALTER STATISTICS r0c_pgd_statistics_s OWNER TO postgres;
ALTER STATISTICS r0c_pgd_statistics_s RENAME TO r0c_pgd_statistics_new;
SELECT stxname,stxstattarget,stxkind FROM pg_statistic_ext WHERE stxname='r0c_pgd_statistics_new';
DROP STATISTICS r0c_pgd_statistics_new;
