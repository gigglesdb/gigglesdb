-- case: pgq_cte_recursive_tree
-- title: cte recursive tree
-- owner: E3
-- expect: compatible
-- source: Differences: WITH and WITH RECURSIVE; PostgreSQL 18 WITH
CREATE TABLE r0c_pgq_cte_recursive_tree (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_cte_recursive_tree VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH RECURSIVE tree(id,depth) AS (SELECT id,0 FROM r0c_pgq_cte_recursive_tree WHERE parent IS NULL UNION ALL SELECT t.id,tree.depth+1 FROM r0c_pgq_cte_recursive_tree t JOIN tree ON t.parent=tree.id) SELECT * FROM tree ORDER BY depth,id;
