-- case: pgd_alter_type_using
-- title: alter type using
-- owner: E1
-- expect: compatible
-- source: Differences: ALTER COLUMN TYPE
CREATE TABLE r0c_pgd_alter_type_using (id int PRIMARY KEY, v text);
INSERT INTO r0c_pgd_alter_type_using VALUES(1,'003');
ALTER TABLE r0c_pgd_alter_type_using ALTER COLUMN v TYPE int USING v::int;
SELECT id,v FROM r0c_pgd_alter_type_using ORDER BY id;
