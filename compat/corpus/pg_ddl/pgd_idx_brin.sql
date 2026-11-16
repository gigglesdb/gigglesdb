-- case: pgd_idx_brin
-- title: idx brin
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_brin (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_idx_brin VALUES (1,1),(2,2);
CREATE INDEX r0c_pgd_idx_brin_i ON r0c_pgd_idx_brin USING brin (v);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_brin_i';
SELECT id,v FROM r0c_pgd_idx_brin ORDER BY id;
DROP INDEX r0c_pgd_idx_brin_i;
