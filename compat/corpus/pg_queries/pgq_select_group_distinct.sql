-- case: pgq_select_group_distinct
-- title: select group distinct
-- owner: E3
-- expect: compatible
-- source: PostgreSQL 18 SELECT DISTINCT aggregates/HAVING
CREATE TABLE r0c_pgq_select_group_distinct (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_select_group_distinct VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT dept,sum(DISTINCT amount),count(DISTINCT amount) FROM r0c_pgq_select_group_distinct GROUP BY dept HAVING sum(amount)>=20 ORDER BY dept;
