-- case: pgd_idx_timestamp_unique
-- title: idx timestamp unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_timestamp_unique (id int PRIMARY KEY, v timestamp);
INSERT INTO r0c_pgd_idx_timestamp_unique VALUES (1,'2024-01-01 12:30'),(2,'2024-02-01 12:30');
CREATE UNIQUE INDEX r0c_pgd_idx_timestamp_unique_i ON r0c_pgd_idx_timestamp_unique (v);
SELECT id,v FROM r0c_pgd_idx_timestamp_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_timestamp_unique VALUES (3,'2024-01-01 12:30');
SELECT count(*) FROM r0c_pgd_idx_timestamp_unique;
DROP INDEX r0c_pgd_idx_timestamp_unique_i;
