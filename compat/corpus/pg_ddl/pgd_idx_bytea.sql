-- case: pgd_idx_bytea
-- title: idx bytea
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_bytea (id int PRIMARY KEY, v bytea);
INSERT INTO r0c_pgd_idx_bytea VALUES (1,decode('ab','hex')),(2,decode('cd','hex'));
CREATE INDEX r0c_pgd_idx_bytea_i ON r0c_pgd_idx_bytea (v);
SELECT id,v FROM r0c_pgd_idx_bytea ORDER BY v,id;
DROP INDEX r0c_pgd_idx_bytea_i;
