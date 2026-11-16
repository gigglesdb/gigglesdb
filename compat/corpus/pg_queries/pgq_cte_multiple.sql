-- case: pgq_cte_multiple
-- title: cte multiple
-- owner: E3
-- expect: compatible
-- source: Differences: WITH and WITH RECURSIVE; PostgreSQL 18 WITH
CREATE TABLE r0c_pgq_cte_multiple (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_cte_multiple VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH paid AS (SELECT * FROM r0c_pgq_cte_multiple WHERE amount IS NOT NULL), totals AS (SELECT dept, sum(amount) AS total FROM paid GROUP BY dept) SELECT * FROM totals ORDER BY dept;
