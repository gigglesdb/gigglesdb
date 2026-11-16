-- case: pgd_idx_hash
-- title: idx hash
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_hash (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_idx_hash VALUES (1,1),(2,2);
CREATE INDEX r0c_pgd_idx_hash_i ON r0c_pgd_idx_hash USING hash (v);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_hash_i';
SELECT id,v FROM r0c_pgd_idx_hash ORDER BY id;
DROP INDEX r0c_pgd_idx_hash_i;
