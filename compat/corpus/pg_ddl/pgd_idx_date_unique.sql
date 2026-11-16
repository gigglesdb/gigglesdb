-- case: pgd_idx_date_unique
-- title: idx date unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_date_unique (id int PRIMARY KEY, v date);
INSERT INTO r0c_pgd_idx_date_unique VALUES (1,'2024-01-01'),(2,'2024-02-01');
CREATE UNIQUE INDEX r0c_pgd_idx_date_unique_i ON r0c_pgd_idx_date_unique (v);
SELECT id,v FROM r0c_pgd_idx_date_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_date_unique VALUES (3,'2024-01-01');
SELECT count(*) FROM r0c_pgd_idx_date_unique;
DROP INDEX r0c_pgd_idx_date_unique_i;
