-- case: pgd_default_arithmetic
-- title: default arithmetic
-- owner: E1
-- expect: compatible
-- source: Differences: arbitrary default expressions
CREATE TABLE r0c_pgd_default_arithmetic (id int PRIMARY KEY, v int DEFAULT (6*7));
INSERT INTO r0c_pgd_default_arithmetic (id) VALUES (1);
SELECT id,v FROM r0c_pgd_default_arithmetic ORDER BY id;
