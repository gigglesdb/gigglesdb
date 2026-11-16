-- case: pgd_idx_uuid_unique
-- title: idx uuid unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_uuid_unique (id int PRIMARY KEY, v uuid);
INSERT INTO r0c_pgd_idx_uuid_unique VALUES (1,'00000000-0000-0000-0000-000000000001'),(2,'00000000-0000-0000-0000-000000000002');
CREATE UNIQUE INDEX r0c_pgd_idx_uuid_unique_i ON r0c_pgd_idx_uuid_unique (v);
SELECT id,v FROM r0c_pgd_idx_uuid_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_uuid_unique VALUES (3,'00000000-0000-0000-0000-000000000001');
SELECT count(*) FROM r0c_pgd_idx_uuid_unique;
DROP INDEX r0c_pgd_idx_uuid_unique_i;
