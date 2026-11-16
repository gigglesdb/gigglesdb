-- case: pgd_idx_uuid
-- title: idx uuid
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_uuid (id int PRIMARY KEY, v uuid);
INSERT INTO r0c_pgd_idx_uuid VALUES (1,'00000000-0000-0000-0000-000000000001'),(2,'00000000-0000-0000-0000-000000000002');
CREATE INDEX r0c_pgd_idx_uuid_i ON r0c_pgd_idx_uuid (v);
SELECT id,v FROM r0c_pgd_idx_uuid ORDER BY v,id;
DROP INDEX r0c_pgd_idx_uuid_i;
