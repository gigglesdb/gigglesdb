-- case: pgd_view_delete
-- title: view delete
-- owner: E1
-- expect: compatible
-- source: Differences: writing through a view
CREATE TABLE r0c_pgd_view_delete (id int PRIMARY KEY, v int);
CREATE VIEW r0c_pgd_view_delete_v AS SELECT id,v FROM r0c_pgd_view_delete WHERE v>10;
INSERT INTO r0c_pgd_view_delete VALUES (1,12),(2,15);
DELETE FROM r0c_pgd_view_delete_v WHERE id=1;
SELECT id,v FROM r0c_pgd_view_delete ORDER BY id;
DROP VIEW r0c_pgd_view_delete_v;
