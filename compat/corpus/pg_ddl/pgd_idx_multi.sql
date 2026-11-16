-- case: pgd_idx_multi
-- title: idx multi
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_multi (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_idx_multi VALUES (1,1),(2,1);
CREATE INDEX r0c_pgd_idx_multi_i ON r0c_pgd_idx_multi (v,id);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_multi_i';
SELECT id,v FROM r0c_pgd_idx_multi ORDER BY id;
DROP INDEX r0c_pgd_idx_multi_i;
