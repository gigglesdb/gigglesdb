-- case: pgd_temp_delete
-- title: temp delete
-- owner: E1
-- expect: compatible
-- source: Differences: temporary tables
BEGIN;
CREATE TEMPORARY TABLE r0c_pgd_temp_delete (id int PRIMARY KEY, v int) ON COMMIT DELETE ROWS;
INSERT INTO r0c_pgd_temp_delete VALUES (1,3);
SELECT * FROM r0c_pgd_temp_delete ORDER BY id;
COMMIT;
SELECT count(*) FROM r0c_pgd_temp_delete;
DROP TABLE r0c_pgd_temp_delete;
