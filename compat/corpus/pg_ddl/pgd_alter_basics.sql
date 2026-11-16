-- case: pgd_alter_basics
-- title: alter basics
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE column and table actions
CREATE TABLE r0c_pgd_alter_basics (id int PRIMARY KEY, v int);
ALTER TABLE r0c_pgd_alter_basics ALTER COLUMN v SET DEFAULT 7;
INSERT INTO r0c_pgd_alter_basics(id) VALUES(1);
ALTER TABLE r0c_pgd_alter_basics ALTER COLUMN v SET NOT NULL;
ALTER TABLE r0c_pgd_alter_basics ALTER COLUMN v DROP NOT NULL;
ALTER TABLE r0c_pgd_alter_basics ALTER COLUMN v DROP DEFAULT;
ALTER TABLE r0c_pgd_alter_basics ADD COLUMN old text;
ALTER TABLE r0c_pgd_alter_basics RENAME COLUMN old TO label;
ALTER TABLE r0c_pgd_alter_basics DROP COLUMN label;
ALTER TABLE r0c_pgd_alter_basics RENAME TO r0c_pgd_alter_basics_renamed;
SELECT * FROM r0c_pgd_alter_basics_renamed ORDER BY id;
DROP TABLE r0c_pgd_alter_basics_renamed;
