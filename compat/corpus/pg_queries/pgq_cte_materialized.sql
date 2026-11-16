-- case: pgq_cte_materialized
-- title: cte materialized
-- owner: E3
-- expect: compatible
-- source: Differences: WITH and WITH RECURSIVE; PostgreSQL 18 WITH
CREATE TABLE r0c_pgq_cte_materialized (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_cte_materialized VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH paid AS MATERIALIZED (SELECT id,amount FROM r0c_pgq_cte_materialized) SELECT * FROM paid WHERE amount>10 ORDER BY id;
