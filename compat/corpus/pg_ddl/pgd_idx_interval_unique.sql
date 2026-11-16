-- case: pgd_idx_interval_unique
-- title: idx interval unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_interval_unique (id int PRIMARY KEY, v interval);
INSERT INTO r0c_pgd_idx_interval_unique VALUES (1,'1 day'),(2,'2 days');
CREATE UNIQUE INDEX r0c_pgd_idx_interval_unique_i ON r0c_pgd_idx_interval_unique (v);
SELECT id,v FROM r0c_pgd_idx_interval_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_interval_unique VALUES (3,'1 day');
SELECT count(*) FROM r0c_pgd_idx_interval_unique;
DROP INDEX r0c_pgd_idx_interval_unique_i;
