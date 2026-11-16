-- case: pgd_view_where
-- title: view where
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_where (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_where VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_where_v AS SELECT id,v FROM r0c_pgd_view_where WHERE v>10;
SELECT * FROM r0c_pgd_view_where_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_where_v;
