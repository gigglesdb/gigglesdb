-- case: pgd_idx_if_exists
-- title: idx if exists
-- owner: G
-- expect: compatible
-- source: PG18 CREATE INDEX IF NOT EXISTS
CREATE TABLE r0c_pgd_idx_if_exists (id int PRIMARY KEY, v int);
CREATE INDEX IF NOT EXISTS r0c_pgd_idx_if_exists_i ON r0c_pgd_idx_if_exists (v);
CREATE INDEX IF NOT EXISTS r0c_pgd_idx_if_exists_i ON r0c_pgd_idx_if_exists (v);
SELECT count(*) FROM pg_indexes WHERE indexname='r0c_pgd_idx_if_exists_i';
DROP INDEX r0c_pgd_idx_if_exists_i;
