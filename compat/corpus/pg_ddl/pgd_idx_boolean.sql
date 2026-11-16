-- case: pgd_idx_boolean
-- title: idx boolean
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_boolean (id int PRIMARY KEY, v boolean);
INSERT INTO r0c_pgd_idx_boolean VALUES (1,false),(2,true);
CREATE INDEX r0c_pgd_idx_boolean_i ON r0c_pgd_idx_boolean (v);
SELECT id,v FROM r0c_pgd_idx_boolean ORDER BY v,id;
DROP INDEX r0c_pgd_idx_boolean_i;
