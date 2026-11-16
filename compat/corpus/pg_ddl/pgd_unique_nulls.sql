-- case: pgd_unique_nulls
-- title: unique nulls
-- owner: E1
-- expect: compatible
-- source: Differences: UNIQUE NULLS NOT DISTINCT
CREATE TABLE r0c_pgd_unique_nulls (id int PRIMARY KEY, v int);
ALTER TABLE r0c_pgd_unique_nulls ADD CONSTRAINT r0c_pgd_unique_nulls_uq UNIQUE NULLS NOT DISTINCT(v);
INSERT INTO r0c_pgd_unique_nulls VALUES (1,NULL);
-- @error
INSERT INTO r0c_pgd_unique_nulls VALUES (2,NULL);
SELECT * FROM r0c_pgd_unique_nulls ORDER BY id;
