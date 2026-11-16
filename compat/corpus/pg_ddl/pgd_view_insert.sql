-- case: pgd_view_insert
-- title: view insert
-- owner: E1
-- expect: compatible
-- source: Differences: writing through a view
CREATE TABLE r0c_pgd_view_insert (id int PRIMARY KEY, v int);
CREATE VIEW r0c_pgd_view_insert_v AS SELECT id,v FROM r0c_pgd_view_insert WHERE v>10;
INSERT INTO r0c_pgd_view_insert_v VALUES (1,12);
SELECT id,v FROM r0c_pgd_view_insert ORDER BY id;
DROP VIEW r0c_pgd_view_insert_v;
