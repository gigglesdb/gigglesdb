-- case: pgd_idx_timestamp
-- title: idx timestamp
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_timestamp (id int PRIMARY KEY, v timestamp);
INSERT INTO r0c_pgd_idx_timestamp VALUES (1,'2024-01-01 12:30'),(2,'2024-02-01 12:30');
CREATE INDEX r0c_pgd_idx_timestamp_i ON r0c_pgd_idx_timestamp (v);
SELECT id,v FROM r0c_pgd_idx_timestamp ORDER BY v,id;
DROP INDEX r0c_pgd_idx_timestamp_i;
