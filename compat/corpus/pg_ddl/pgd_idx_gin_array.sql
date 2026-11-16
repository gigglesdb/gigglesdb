-- case: pgd_idx_gin_array
-- title: idx gin array
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_gin_array (id int PRIMARY KEY, v int[]);
INSERT INTO r0c_pgd_idx_gin_array VALUES (1,'{1,2}'),(2,'{2,3}');
CREATE INDEX r0c_pgd_idx_gin_array_i ON r0c_pgd_idx_gin_array USING gin (v);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_gin_array_i';
SELECT id,v FROM r0c_pgd_idx_gin_array ORDER BY id;
DROP INDEX r0c_pgd_idx_gin_array_i;
