-- case: pgd_idx_include
-- title: idx include
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_include (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_idx_include VALUES (1,1),(2,2);
CREATE INDEX r0c_pgd_idx_include_i ON r0c_pgd_idx_include (v) INCLUDE (id);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_include_i';
SELECT id,v FROM r0c_pgd_idx_include ORDER BY id;
DROP INDEX r0c_pgd_idx_include_i;
