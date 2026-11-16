-- case: pgq_having_filter
-- title: having filter
-- owner: E3
-- expect: compatible
-- source: Differences: having filter; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_having_filter (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_having_filter VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT dept,count(*),sum(amount) FILTER (WHERE amount>10) FROM r0c_pgq_having_filter WHERE id>1 GROUP BY dept HAVING count(*)>=1 ORDER BY dept;
