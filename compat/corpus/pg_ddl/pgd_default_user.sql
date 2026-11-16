-- case: pgd_default_user
-- title: default user
-- owner: E1
-- expect: compatible
-- source: Differences: arbitrary default expressions
CREATE TABLE r0c_pgd_default_user (id int PRIMARY KEY, v text DEFAULT CURRENT_USER);
INSERT INTO r0c_pgd_default_user (id) VALUES (1);
SELECT id,v = CURRENT_USER FROM r0c_pgd_default_user ORDER BY id;
