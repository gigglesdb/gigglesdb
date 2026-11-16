-- case: pgd_temp_drop
-- title: temp drop
-- owner: E1
-- expect: compatible
-- source: Differences: temporary tables
BEGIN;
CREATE TEMPORARY TABLE r0c_pgd_temp_drop (id int PRIMARY KEY, v int) ON COMMIT DROP;
INSERT INTO r0c_pgd_temp_drop VALUES (1,3);
SELECT * FROM r0c_pgd_temp_drop ORDER BY id;
COMMIT;
SELECT to_regclass('pg_temp.r0c_pgd_temp_drop') IS NULL AS dropped;
