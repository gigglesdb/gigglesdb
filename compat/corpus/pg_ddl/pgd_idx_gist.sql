-- case: pgd_idx_gist
-- title: idx gist
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_gist (id int PRIMARY KEY, v int4range);
INSERT INTO r0c_pgd_idx_gist VALUES (1,'[1,4)'),(2,'[5,9)');
CREATE INDEX r0c_pgd_idx_gist_i ON r0c_pgd_idx_gist USING gist (v);
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_gist_i';
SELECT id,v FROM r0c_pgd_idx_gist ORDER BY id;
DROP INDEX r0c_pgd_idx_gist_i;
