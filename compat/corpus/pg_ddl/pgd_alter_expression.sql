-- case: pgd_alter_expression
-- title: alter expression
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE actions
CREATE TABLE r0c_pgd_alter_expression (id int PRIMARY KEY,v int,total int GENERATED ALWAYS AS (v*2) STORED);
INSERT INTO r0c_pgd_alter_expression(id,v) VALUES(1,4);
ALTER TABLE r0c_pgd_alter_expression ALTER COLUMN total SET EXPRESSION AS (v*3);
SELECT * FROM r0c_pgd_alter_expression ORDER BY id;
ALTER TABLE r0c_pgd_alter_expression ALTER COLUMN total DROP EXPRESSION;
UPDATE r0c_pgd_alter_expression SET total=99;
SELECT * FROM r0c_pgd_alter_expression ORDER BY id;
