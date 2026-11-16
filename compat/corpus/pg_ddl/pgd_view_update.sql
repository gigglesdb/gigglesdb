-- case: pgd_view_update
-- title: view update
-- owner: E1
-- expect: compatible
-- source: Differences: writing through a view
CREATE TABLE r0c_pgd_view_update (id int PRIMARY KEY, v int);
CREATE VIEW r0c_pgd_view_update_v AS SELECT id,v FROM r0c_pgd_view_update WHERE v>10;
INSERT INTO r0c_pgd_view_update VALUES (1,12);
UPDATE r0c_pgd_view_update_v SET v=25 WHERE id=1;
SELECT id,v FROM r0c_pgd_view_update ORDER BY id;
DROP VIEW r0c_pgd_view_update_v;
