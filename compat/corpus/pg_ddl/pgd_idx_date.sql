-- case: pgd_idx_date
-- title: idx date
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_date (id int PRIMARY KEY, v date);
INSERT INTO r0c_pgd_idx_date VALUES (1,'2024-01-01'),(2,'2024-02-01');
CREATE INDEX r0c_pgd_idx_date_i ON r0c_pgd_idx_date (v);
SELECT id,v FROM r0c_pgd_idx_date ORDER BY v,id;
DROP INDEX r0c_pgd_idx_date_i;
