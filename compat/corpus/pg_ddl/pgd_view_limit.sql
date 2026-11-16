-- case: pgd_view_limit
-- title: view limit
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_limit (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_limit VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_limit_v AS SELECT id,v FROM r0c_pgd_view_limit ORDER BY id LIMIT 1;
SELECT * FROM r0c_pgd_view_limit_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_limit_v;
