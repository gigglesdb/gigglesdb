-- case: pgd_idx_partial
-- title: idx partial
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_partial (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_idx_partial VALUES (1,1),(2,2);
CREATE INDEX r0c_pgd_idx_partial_i ON r0c_pgd_idx_partial (v) WHERE v > 1;
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_partial_i';
SELECT id,v FROM r0c_pgd_idx_partial ORDER BY id;
DROP INDEX r0c_pgd_idx_partial_i;
