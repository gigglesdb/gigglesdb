-- case: pgd_idx_timestamptz
-- title: idx timestamptz
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_timestamptz (id int PRIMARY KEY, v timestamptz);
INSERT INTO r0c_pgd_idx_timestamptz VALUES (1,'2024-01-01 12:30+00'),(2,'2024-02-01 12:30+00');
CREATE INDEX r0c_pgd_idx_timestamptz_i ON r0c_pgd_idx_timestamptz (v);
SELECT id,v FROM r0c_pgd_idx_timestamptz ORDER BY v,id;
DROP INDEX r0c_pgd_idx_timestamptz_i;
