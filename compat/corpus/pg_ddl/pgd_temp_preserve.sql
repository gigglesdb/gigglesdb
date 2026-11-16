-- case: pgd_temp_preserve
-- title: temp preserve
-- owner: E1
-- expect: compatible
-- source: Differences: temporary tables
BEGIN;
CREATE TEMP TABLE r0c_pgd_temp_preserve (id int PRIMARY KEY,v text) ON COMMIT PRESERVE ROWS;
INSERT INTO r0c_pgd_temp_preserve VALUES(1,'kept');
COMMIT;
SELECT * FROM r0c_pgd_temp_preserve ORDER BY id;
DROP TABLE r0c_pgd_temp_preserve;
