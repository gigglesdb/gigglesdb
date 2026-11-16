-- case: pgq_cte_plain
-- title: cte plain
-- owner: E3
-- expect: compatible
-- source: Differences: WITH and WITH RECURSIVE; PostgreSQL 18 WITH
CREATE TABLE r0c_pgq_cte_plain (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_cte_plain VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH paid AS (SELECT id, amount FROM r0c_pgq_cte_plain WHERE amount >= 20) SELECT * FROM paid ORDER BY id;
