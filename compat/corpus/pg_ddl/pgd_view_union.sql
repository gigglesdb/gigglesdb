-- case: pgd_view_union
-- title: view union
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_union (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_union VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_union_v AS SELECT id,v FROM r0c_pgd_view_union WHERE id=1 UNION SELECT id,v FROM r0c_pgd_view_union WHERE id=2;
SELECT * FROM r0c_pgd_view_union_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_union_v;
