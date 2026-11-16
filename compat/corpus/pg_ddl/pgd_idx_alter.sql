-- case: pgd_idx_alter
-- title: idx alter
-- owner: G
-- expect: compatible
-- source: Differences: ALTER INDEX
CREATE TABLE r0c_pgd_idx_alter (id int PRIMARY KEY, v int);
CREATE INDEX r0c_pgd_idx_alter_i ON r0c_pgd_idx_alter (v);
ALTER INDEX r0c_pgd_idx_alter_i RENAME TO r0c_pgd_idx_alter_renamed;
ALTER INDEX r0c_pgd_idx_alter_renamed SET TABLESPACE pg_default;
ALTER INDEX r0c_pgd_idx_alter_renamed SET (fillfactor=80);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_alter_renamed';
DROP INDEX r0c_pgd_idx_alter_renamed;
