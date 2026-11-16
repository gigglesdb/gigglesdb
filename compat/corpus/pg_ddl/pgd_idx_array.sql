-- case: pgd_idx_array
-- title: idx array
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_array (id int PRIMARY KEY, v int[]);
INSERT INTO r0c_pgd_idx_array VALUES (1,'{1,2}'),(2,'{3,4}');
CREATE INDEX r0c_pgd_idx_array_i ON r0c_pgd_idx_array (v);
SELECT id,v FROM r0c_pgd_idx_array ORDER BY v,id;
DROP INDEX r0c_pgd_idx_array_i;
