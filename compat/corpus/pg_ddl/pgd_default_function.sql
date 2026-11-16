-- case: pgd_default_function
-- title: default function
-- owner: E1
-- expect: compatible
-- source: Differences: arbitrary default expressions
CREATE TABLE r0c_pgd_default_function (id int PRIMARY KEY, v text DEFAULT upper('pending'));
INSERT INTO r0c_pgd_default_function (id) VALUES (1);
SELECT id,v FROM r0c_pgd_default_function ORDER BY id;
