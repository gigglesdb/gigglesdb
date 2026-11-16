-- case: pgd_alter_type_narrow
-- title: alter type narrow
-- owner: E1
-- expect: compatible
-- source: Differences: ALTER COLUMN TYPE
CREATE TABLE r0c_pgd_alter_type_narrow (id int PRIMARY KEY, v varchar(20));
INSERT INTO r0c_pgd_alter_type_narrow VALUES(1,'toolong');
-- @error
ALTER TABLE r0c_pgd_alter_type_narrow ALTER COLUMN v TYPE varchar(3);
SELECT id,v FROM r0c_pgd_alter_type_narrow ORDER BY id;
