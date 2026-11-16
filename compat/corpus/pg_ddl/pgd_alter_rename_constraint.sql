-- case: pgd_alter_rename_constraint
-- title: alter rename constraint
-- owner: G
-- expect: compatible
-- source: Differences: RENAME CONSTRAINT
CREATE TABLE r0c_pgd_alter_rename_constraint (id int PRIMARY KEY, v int);
ALTER TABLE r0c_pgd_alter_rename_constraint ADD CONSTRAINT r0c_pgd_alter_rename_constraint_positive CHECK(v>0);
ALTER TABLE r0c_pgd_alter_rename_constraint RENAME CONSTRAINT r0c_pgd_alter_rename_constraint_positive TO r0c_pgd_alter_rename_constraint_check;
-- @error
INSERT INTO r0c_pgd_alter_rename_constraint VALUES(1,-1);
SELECT conname,convalidated FROM pg_constraint WHERE conrelid='r0c_pgd_alter_rename_constraint'::regclass ORDER BY conname;
