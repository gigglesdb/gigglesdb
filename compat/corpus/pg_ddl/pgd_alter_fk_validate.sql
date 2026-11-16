-- case: pgd_alter_fk_validate
-- title: alter fk validate
-- owner: E1
-- expect: compatible
-- source: PG18 foreign key NOT VALID
CREATE TABLE r0c_pgd_alter_fk_validate (id int PRIMARY KEY);
CREATE TABLE r0c_pgd_alter_fk_validate_c (id int PRIMARY KEY,pid int);
INSERT INTO r0c_pgd_alter_fk_validate_c VALUES(1,9);
ALTER TABLE r0c_pgd_alter_fk_validate_c ADD CONSTRAINT r0c_pgd_alter_fk_validate_fk FOREIGN KEY(pid) REFERENCES r0c_pgd_alter_fk_validate(id) NOT VALID;
INSERT INTO r0c_pgd_alter_fk_validate VALUES(9);
ALTER TABLE r0c_pgd_alter_fk_validate_c VALIDATE CONSTRAINT r0c_pgd_alter_fk_validate_fk;
SELECT * FROM r0c_pgd_alter_fk_validate_c ORDER BY id;
DROP TABLE r0c_pgd_alter_fk_validate_c;
