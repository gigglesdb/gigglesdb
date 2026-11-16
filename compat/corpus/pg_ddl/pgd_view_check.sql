-- case: pgd_view_check
-- title: view check
-- owner: E1
-- expect: compatible
-- source: PG18 WITH CHECK OPTION
CREATE TABLE r0c_pgd_view_check (id int PRIMARY KEY, v int);
CREATE VIEW r0c_pgd_view_check_v AS SELECT * FROM r0c_pgd_view_check WHERE v>10 WITH CASCADED CHECK OPTION;
INSERT INTO r0c_pgd_view_check_v VALUES (1,12);
-- @error
INSERT INTO r0c_pgd_view_check_v VALUES (2,1);
-- @error
UPDATE r0c_pgd_view_check_v SET v=0 WHERE id=1;
SELECT * FROM r0c_pgd_view_check ORDER BY id;
DROP VIEW r0c_pgd_view_check_v;
