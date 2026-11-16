-- case: pgd_idx_double
-- title: idx double
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_double (id int PRIMARY KEY, v double precision);
INSERT INTO r0c_pgd_idx_double VALUES (1,1.25),(2,2.5);
CREATE INDEX r0c_pgd_idx_double_i ON r0c_pgd_idx_double (v);
SELECT id,v FROM r0c_pgd_idx_double ORDER BY v,id;
DROP INDEX r0c_pgd_idx_double_i;
