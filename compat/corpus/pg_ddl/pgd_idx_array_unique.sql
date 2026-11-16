-- case: pgd_idx_array_unique
-- title: idx array unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_array_unique (id int PRIMARY KEY, v int[]);
INSERT INTO r0c_pgd_idx_array_unique VALUES (1,'{1,2}'),(2,'{3,4}');
CREATE UNIQUE INDEX r0c_pgd_idx_array_unique_i ON r0c_pgd_idx_array_unique (v);
SELECT id,v FROM r0c_pgd_idx_array_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_array_unique VALUES (3,'{1,2}');
SELECT count(*) FROM r0c_pgd_idx_array_unique;
DROP INDEX r0c_pgd_idx_array_unique_i;
