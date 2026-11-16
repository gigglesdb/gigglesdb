-- case: pgd_view_expression
-- title: view expression
-- owner: E1
-- expect: compatible
-- source: Differences: views with query clauses
CREATE TABLE r0c_pgd_view_expression (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_view_expression VALUES (1,10),(2,20),(3,20);
CREATE VIEW r0c_pgd_view_expression_v AS SELECT id,v*2 AS doubled FROM r0c_pgd_view_expression;
SELECT * FROM r0c_pgd_view_expression_v ORDER BY 1,2;
DROP VIEW r0c_pgd_view_expression_v;
