-- case: pgd_idx_concurrent
-- title: idx concurrent
-- owner: E1
-- expect: compatible
-- source: Differences: CREATE INDEX CONCURRENTLY
CREATE TABLE r0c_pgd_idx_concurrent (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_idx_concurrent VALUES (1,10);
CREATE INDEX CONCURRENTLY r0c_pgd_idx_concurrent_i ON r0c_pgd_idx_concurrent (v);
SELECT id FROM r0c_pgd_idx_concurrent WHERE v=10 ORDER BY id;
DROP INDEX CONCURRENTLY r0c_pgd_idx_concurrent_i;
