-- case: pgd_view_cte
-- title: view cte
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_cte (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_cte VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_cte_v AS WITH c AS (SELECT id,v FROM r0c_pgd_view_cte WHERE id=1) SELECT * FROM c;
SELECT * FROM r0c_pgd_view_cte_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_cte_v;
