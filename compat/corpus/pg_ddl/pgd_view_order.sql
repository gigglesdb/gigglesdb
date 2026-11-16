-- case: pgd_view_order
-- title: view order
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_order (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_order VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_order_v AS SELECT id,v FROM r0c_pgd_view_order ORDER BY v DESC,id;
SELECT * FROM r0c_pgd_view_order_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_order_v;
