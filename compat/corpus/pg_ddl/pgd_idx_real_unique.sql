-- case: pgd_idx_real_unique
-- title: idx real unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_real_unique (id int PRIMARY KEY, v real);
INSERT INTO r0c_pgd_idx_real_unique VALUES (1,1.25),(2,2.5);
CREATE UNIQUE INDEX r0c_pgd_idx_real_unique_i ON r0c_pgd_idx_real_unique (v);
SELECT id,v FROM r0c_pgd_idx_real_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_real_unique VALUES (3,1.25);
SELECT count(*) FROM r0c_pgd_idx_real_unique;
DROP INDEX r0c_pgd_idx_real_unique_i;
