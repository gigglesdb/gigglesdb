-- case: pgd_view_join
-- title: view join
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_join (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_join VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_join_v AS SELECT a.id,a.v+b.v AS total FROM r0c_pgd_view_join a JOIN r0c_pgd_view_join b ON a.id=b.id;
SELECT * FROM r0c_pgd_view_join_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_join_v;
