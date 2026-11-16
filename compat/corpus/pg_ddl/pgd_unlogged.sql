-- case: pgd_unlogged
-- title: unlogged
-- owner: E1
-- expect: compatible
-- source: Differences: unlogged tables
CREATE UNLOGGED TABLE r0c_pgd_unlogged (id int PRIMARY KEY, v text);
INSERT INTO r0c_pgd_unlogged VALUES (1,'cache');
SELECT * FROM r0c_pgd_unlogged ORDER BY id;
ALTER TABLE r0c_pgd_unlogged SET LOGGED;
ALTER TABLE r0c_pgd_unlogged SET UNLOGGED;
DROP TABLE r0c_pgd_unlogged;
