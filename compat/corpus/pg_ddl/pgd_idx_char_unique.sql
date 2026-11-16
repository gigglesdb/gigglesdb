-- case: pgd_idx_char_unique
-- title: idx char unique
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_char_unique (id int PRIMARY KEY, v char(4));
INSERT INTO r0c_pgd_idx_char_unique VALUES (1,'ab'),(2,'cd');
CREATE UNIQUE INDEX r0c_pgd_idx_char_unique_i ON r0c_pgd_idx_char_unique (v);
SELECT id,v FROM r0c_pgd_idx_char_unique ORDER BY v,id;
-- @error
INSERT INTO r0c_pgd_idx_char_unique VALUES (3,'ab');
SELECT count(*) FROM r0c_pgd_idx_char_unique;
DROP INDEX r0c_pgd_idx_char_unique_i;
