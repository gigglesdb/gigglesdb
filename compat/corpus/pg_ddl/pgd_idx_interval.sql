-- case: pgd_idx_interval
-- title: idx interval
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_interval (id int PRIMARY KEY, v interval);
INSERT INTO r0c_pgd_idx_interval VALUES (1,'1 day'),(2,'2 days');
CREATE INDEX r0c_pgd_idx_interval_i ON r0c_pgd_idx_interval (v);
SELECT id,v FROM r0c_pgd_idx_interval ORDER BY v,id;
DROP INDEX r0c_pgd_idx_interval_i;
