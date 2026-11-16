-- case: pgd_idx_time
-- title: idx time
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_time (id int PRIMARY KEY, v time);
INSERT INTO r0c_pgd_idx_time VALUES (1,'12:30'),(2,'13:30');
CREATE INDEX r0c_pgd_idx_time_i ON r0c_pgd_idx_time (v);
SELECT id,v FROM r0c_pgd_idx_time ORDER BY v,id;
DROP INDEX r0c_pgd_idx_time_i;
