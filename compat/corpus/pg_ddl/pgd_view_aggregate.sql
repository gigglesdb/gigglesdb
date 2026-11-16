-- case: pgd_view_aggregate
-- title: view aggregate
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_aggregate (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_aggregate VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_aggregate_v AS SELECT v,count(*) AS n FROM r0c_pgd_view_aggregate GROUP BY v;
SELECT * FROM r0c_pgd_view_aggregate_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_aggregate_v;
