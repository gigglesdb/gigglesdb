-- case: pgq_cte_cycle
-- title: cte cycle
-- owner: E3
-- expect: compatible
-- source: Differences: WITH and WITH RECURSIVE; PostgreSQL 18 WITH
CREATE TABLE r0c_pgq_cte_cycle (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_cte_cycle VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
UPDATE r0c_pgq_cte_cycle SET parent=4 WHERE id=1;
WITH RECURSIVE tree(id,parent) AS (SELECT id,parent FROM r0c_pgq_cte_cycle WHERE id=1 UNION ALL SELECT t.id,t.parent FROM r0c_pgq_cte_cycle t JOIN tree ON t.parent=tree.id) CYCLE id SET is_cycle USING path SELECT id,is_cycle FROM tree ORDER BY path;
