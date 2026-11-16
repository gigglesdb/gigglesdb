-- case: pgd_idx_expression
-- title: idx expression
-- owner: G
-- expect: compatible
-- source: Differences: expression partial INCLUDE descending and access methods
CREATE TABLE r0c_pgd_idx_expression (id int PRIMARY KEY, v text);
INSERT INTO r0c_pgd_idx_expression VALUES (1,'Ada'),(2,'ADA');
CREATE INDEX r0c_pgd_idx_expression_i ON r0c_pgd_idx_expression (lower(v));
SELECT indexdef FROM pg_indexes WHERE indexname='r0c_pgd_idx_expression_i';
SELECT id,v FROM r0c_pgd_idx_expression ORDER BY id;
DROP INDEX r0c_pgd_idx_expression_i;
