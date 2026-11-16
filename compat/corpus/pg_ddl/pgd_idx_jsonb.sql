-- case: pgd_idx_jsonb
-- title: idx jsonb
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_jsonb (id int PRIMARY KEY, v jsonb);
INSERT INTO r0c_pgd_idx_jsonb VALUES (1,'{"a":1}'),(2,'{"a":2}');
CREATE INDEX r0c_pgd_idx_jsonb_i ON r0c_pgd_idx_jsonb (v);
SELECT id,v FROM r0c_pgd_idx_jsonb ORDER BY v,id;
DROP INDEX r0c_pgd_idx_jsonb_i;
