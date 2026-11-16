-- case: pgd_alter_constraint
-- title: alter constraint
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE ALTER CONSTRAINT
CREATE TABLE r0c_pgd_alter_constraint (id int PRIMARY KEY);
CREATE TABLE r0c_pgd_alter_constraint_c (id int PRIMARY KEY,pid int REFERENCES r0c_pgd_alter_constraint(id));
ALTER TABLE r0c_pgd_alter_constraint_c ALTER CONSTRAINT r0c_pgd_alter_constraint_c_pid_fkey DEFERRABLE INITIALLY DEFERRED;
BEGIN;
INSERT INTO r0c_pgd_alter_constraint_c VALUES(1,7);
INSERT INTO r0c_pgd_alter_constraint VALUES(7);
COMMIT;
SELECT * FROM r0c_pgd_alter_constraint_c ORDER BY id;
DROP TABLE r0c_pgd_alter_constraint_c;
