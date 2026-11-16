-- case: pgd_alter_not_null
-- title: alter not null
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE actions
CREATE TABLE r0c_pgd_alter_not_null (id int PRIMARY KEY,v int);
INSERT INTO r0c_pgd_alter_not_null VALUES(1,NULL);
-- @error
ALTER TABLE r0c_pgd_alter_not_null ALTER COLUMN v SET NOT NULL;
UPDATE r0c_pgd_alter_not_null SET v=7 WHERE id=1;
ALTER TABLE r0c_pgd_alter_not_null ALTER COLUMN v SET NOT NULL;
-- @error
INSERT INTO r0c_pgd_alter_not_null VALUES(2,NULL);
SELECT * FROM r0c_pgd_alter_not_null ORDER BY id;
