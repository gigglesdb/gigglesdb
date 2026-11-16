-- case: pgd_idx_boolean_unique
-- title: idx boolean unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_boolean_unique (id int PRIMARY KEY, v boolean);
INSERT INTO r0c_pgd_idx_boolean_unique VALUES (1,false),(2,true);
CREATE UNIQUE INDEX r0c_pgd_idx_boolean_unique_i ON r0c_pgd_idx_boolean_unique (v);
SELECT id,v FROM r0c_pgd_idx_boolean_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_boolean_unique VALUES (3,false);
SELECT count(*) FROM r0c_pgd_idx_boolean_unique;
DROP INDEX r0c_pgd_idx_boolean_unique_i;
