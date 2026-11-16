-- case: pgd_view_distinct
-- title: view distinct
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_distinct (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_distinct VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_distinct_v AS SELECT DISTINCT v FROM r0c_pgd_view_distinct;
SELECT * FROM r0c_pgd_view_distinct_v ORDER BY 1;
DROP VIEW r0c_pgd_view_distinct_v;
