-- case: pgd_alter_if_exists
-- title: alter if exists
-- owner: E1
-- expect: compatible
-- source: Differences: ALTER TABLE IF EXISTS
ALTER TABLE IF EXISTS r0c_pgd_alter_if_exists_missing ADD COLUMN v int;
CREATE TABLE r0c_pgd_alter_if_exists (id int PRIMARY KEY, v int);
ALTER TABLE IF EXISTS r0c_pgd_alter_if_exists ADD COLUMN IF NOT EXISTS tag text DEFAULT 'new';
ALTER TABLE r0c_pgd_alter_if_exists ADD COLUMN IF NOT EXISTS tag text;
INSERT INTO r0c_pgd_alter_if_exists VALUES(1,3,'yes');
ALTER TABLE r0c_pgd_alter_if_exists DROP COLUMN IF EXISTS absent;
SELECT * FROM r0c_pgd_alter_if_exists ORDER BY id;
