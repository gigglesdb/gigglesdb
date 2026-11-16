-- case: pgq_cte_search_depth
-- title: cte search depth
-- owner: E3
-- expect: compatible
-- source: Differences: WITH and WITH RECURSIVE; PostgreSQL 18 WITH
CREATE TABLE r0c_pgq_cte_search_depth (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_cte_search_depth VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH RECURSIVE tree(id,parent) AS (SELECT id,parent FROM r0c_pgq_cte_search_depth WHERE parent IS NULL UNION ALL SELECT t.id,t.parent FROM r0c_pgq_cte_search_depth t JOIN tree ON t.parent=tree.id) SEARCH DEPTH FIRST BY id SET visit SELECT id FROM tree ORDER BY visit;
