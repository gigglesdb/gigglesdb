-- case: pgd_idx_gin_jsonb
-- title: idx gin jsonb
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_gin_jsonb (id int PRIMARY KEY, v jsonb);
INSERT INTO r0c_pgd_idx_gin_jsonb VALUES (1,'{"a":1}'),(2,'{"a":2}');
CREATE INDEX r0c_pgd_idx_gin_jsonb_i ON r0c_pgd_idx_gin_jsonb USING gin (v);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_gin_jsonb_i';
SELECT id,v FROM r0c_pgd_idx_gin_jsonb ORDER BY id;
DROP INDEX r0c_pgd_idx_gin_jsonb_i;
