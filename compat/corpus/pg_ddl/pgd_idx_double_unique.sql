-- case: pgd_idx_double_unique
-- title: idx double unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_double_unique (id int PRIMARY KEY, v double precision);
INSERT INTO r0c_pgd_idx_double_unique VALUES (1,1.25),(2,2.5);
CREATE UNIQUE INDEX r0c_pgd_idx_double_unique_i ON r0c_pgd_idx_double_unique (v);
SELECT id,v FROM r0c_pgd_idx_double_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_double_unique VALUES (3,1.25);
SELECT count(*) FROM r0c_pgd_idx_double_unique;
DROP INDEX r0c_pgd_idx_double_unique_i;
