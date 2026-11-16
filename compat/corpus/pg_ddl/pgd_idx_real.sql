-- case: pgd_idx_real
-- title: idx real
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_real (id int PRIMARY KEY, v real);
INSERT INTO r0c_pgd_idx_real VALUES (1,1.25),(2,2.5);
CREATE INDEX r0c_pgd_idx_real_i ON r0c_pgd_idx_real (v);
SELECT id,v FROM r0c_pgd_idx_real ORDER BY v,id;
DROP INDEX r0c_pgd_idx_real_i;
