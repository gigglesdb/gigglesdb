-- case: pgd_alter_type_widen
-- title: alter type widen
-- owner: E1
-- expect: compatible
-- source: Differences: ALTER COLUMN TYPE
CREATE TABLE r0c_pgd_alter_type_widen (id int PRIMARY KEY, v varchar(3));
INSERT INTO r0c_pgd_alter_type_widen VALUES(1,'abc');
ALTER TABLE r0c_pgd_alter_type_widen ALTER COLUMN v TYPE varchar(20);
SELECT id,v FROM r0c_pgd_alter_type_widen ORDER BY id;
