-- case: pgd_alter_validate
-- title: alter validate
-- owner: E1
-- expect: compatible
-- source: PG18 NOT VALID and VALIDATE CONSTRAINT
CREATE TABLE r0c_pgd_alter_validate (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_alter_validate VALUES(1,-1);
ALTER TABLE r0c_pgd_alter_validate ADD CONSTRAINT r0c_pgd_alter_validate_ck CHECK(v>0) NOT VALID;
-- @error
INSERT INTO r0c_pgd_alter_validate VALUES(2,-1);
-- @error
ALTER TABLE r0c_pgd_alter_validate VALIDATE CONSTRAINT r0c_pgd_alter_validate_ck;
UPDATE r0c_pgd_alter_validate SET v=1 WHERE id=1;
ALTER TABLE r0c_pgd_alter_validate VALIDATE CONSTRAINT r0c_pgd_alter_validate_ck;
SELECT * FROM r0c_pgd_alter_validate ORDER BY id;
ALTER TABLE r0c_pgd_alter_validate DROP CONSTRAINT r0c_pgd_alter_validate_ck;
