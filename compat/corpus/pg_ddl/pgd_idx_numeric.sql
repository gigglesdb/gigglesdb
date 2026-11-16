-- case: pgd_idx_numeric
-- title: idx numeric
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_numeric (id int PRIMARY KEY, v numeric);
INSERT INTO r0c_pgd_idx_numeric VALUES (1,1.25),(2,2.5);
CREATE INDEX r0c_pgd_idx_numeric_i ON r0c_pgd_idx_numeric (v);
SELECT id,v FROM r0c_pgd_idx_numeric ORDER BY v,id;
DROP INDEX r0c_pgd_idx_numeric_i;
