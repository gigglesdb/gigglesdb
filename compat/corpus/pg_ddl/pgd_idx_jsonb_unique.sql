-- case: pgd_idx_jsonb_unique
-- title: idx jsonb unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_jsonb_unique (id int PRIMARY KEY, v jsonb);
INSERT INTO r0c_pgd_idx_jsonb_unique VALUES (1,'{"a":1}'),(2,'{"a":2}');
CREATE UNIQUE INDEX r0c_pgd_idx_jsonb_unique_i ON r0c_pgd_idx_jsonb_unique (v);
SELECT id,v FROM r0c_pgd_idx_jsonb_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_jsonb_unique VALUES (3,'{"a":1}');
SELECT count(*) FROM r0c_pgd_idx_jsonb_unique;
DROP INDEX r0c_pgd_idx_jsonb_unique_i;
