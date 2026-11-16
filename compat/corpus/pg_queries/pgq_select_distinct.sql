-- case: pgq_select_distinct
-- title: select distinct
-- owner: E3
-- expect: compatible
-- source: PostgreSQL 18 SELECT / DML; Differences: window functions and data changes
CREATE TABLE r0c_pgq_select_distinct (id int PRIMARY KEY, dept text, amount int);
INSERT INTO r0c_pgq_select_distinct VALUES (1,'a',10),(2,'a',20),(3,'b',20),(4,'b',NULL);
SELECT DISTINCT dept,amount FROM r0c_pgq_select_distinct ORDER BY dept,amount NULLS LAST;
