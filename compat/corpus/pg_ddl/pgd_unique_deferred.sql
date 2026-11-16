-- case: pgd_unique_deferred
-- title: unique deferred
-- owner: E1
-- expect: compatible
-- source: Differences: deferrable UNIQUE
CREATE TABLE r0c_pgd_unique_deferred (id int PRIMARY KEY, v int);
ALTER TABLE r0c_pgd_unique_deferred ADD CONSTRAINT r0c_pgd_unique_deferred_uq UNIQUE(v) DEFERRABLE INITIALLY DEFERRED;
INSERT INTO r0c_pgd_unique_deferred VALUES (1,10),(2,20);
BEGIN;
SET CONSTRAINTS r0c_pgd_unique_deferred_uq DEFERRED;
UPDATE r0c_pgd_unique_deferred SET v=20 WHERE id=1;
UPDATE r0c_pgd_unique_deferred SET v=10 WHERE id=2;
SET CONSTRAINTS r0c_pgd_unique_deferred_uq IMMEDIATE;
COMMIT;
SELECT * FROM r0c_pgd_unique_deferred ORDER BY id;
BEGIN;
INSERT INTO r0c_pgd_unique_deferred VALUES(3,10);
-- @error
COMMIT;
SELECT * FROM r0c_pgd_unique_deferred ORDER BY id;
