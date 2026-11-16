-- case: pgd_idx_descending
-- title: idx descending
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_descending (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_idx_descending VALUES (1,NULL),(2,2);
CREATE INDEX r0c_pgd_idx_descending_i ON r0c_pgd_idx_descending (v DESC NULLS FIRST);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_descending_i';
SELECT id,v FROM r0c_pgd_idx_descending ORDER BY id;
DROP INDEX r0c_pgd_idx_descending_i;
