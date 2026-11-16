-- case: pgd_idx_time_unique
-- title: idx time unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_time_unique (id int PRIMARY KEY, v time);
INSERT INTO r0c_pgd_idx_time_unique VALUES (1,'12:30'),(2,'13:30');
CREATE UNIQUE INDEX r0c_pgd_idx_time_unique_i ON r0c_pgd_idx_time_unique (v);
SELECT id,v FROM r0c_pgd_idx_time_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_time_unique VALUES (3,'12:30');
SELECT count(*) FROM r0c_pgd_idx_time_unique;
DROP INDEX r0c_pgd_idx_time_unique_i;
