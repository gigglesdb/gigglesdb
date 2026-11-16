-- case: pgd_idx_bytea_unique
-- title: idx bytea unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_bytea_unique (id int PRIMARY KEY, v bytea);
INSERT INTO r0c_pgd_idx_bytea_unique VALUES (1,decode('ab','hex')),(2,decode('cd','hex'));
CREATE UNIQUE INDEX r0c_pgd_idx_bytea_unique_i ON r0c_pgd_idx_bytea_unique (v);
SELECT id,v FROM r0c_pgd_idx_bytea_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_bytea_unique VALUES (3,decode('ab','hex'));
SELECT count(*) FROM r0c_pgd_idx_bytea_unique;
DROP INDEX r0c_pgd_idx_bytea_unique_i;
