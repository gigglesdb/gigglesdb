-- case: pgd_idx_reindex
-- title: idx reindex
-- owner: E1
-- expect: compatible
-- source: Differences: REINDEX
CREATE TABLE r0c_pgd_idx_reindex (id int PRIMARY KEY, v int);
CREATE INDEX r0c_pgd_idx_reindex_i ON r0c_pgd_idx_reindex (v);
INSERT INTO r0c_pgd_idx_reindex VALUES (1,7);
REINDEX INDEX r0c_pgd_idx_reindex_i;
REINDEX TABLE r0c_pgd_idx_reindex;
REINDEX INDEX CONCURRENTLY r0c_pgd_idx_reindex_i;
SELECT id,v FROM r0c_pgd_idx_reindex ORDER BY id;
DROP INDEX r0c_pgd_idx_reindex_i;
