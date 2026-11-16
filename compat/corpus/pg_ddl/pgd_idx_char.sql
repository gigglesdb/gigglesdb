-- case: pgd_idx_char
-- title: idx char
-- owner: E1
-- expect: compatible
-- source: Differences: indexes on non-integer types
CREATE TABLE r0c_pgd_idx_char (id int PRIMARY KEY, v char(4));
INSERT INTO r0c_pgd_idx_char VALUES (1,'ab'),(2,'cd');
CREATE INDEX r0c_pgd_idx_char_i ON r0c_pgd_idx_char (v);
SELECT id,v FROM r0c_pgd_idx_char ORDER BY v,id;
DROP INDEX r0c_pgd_idx_char_i;
