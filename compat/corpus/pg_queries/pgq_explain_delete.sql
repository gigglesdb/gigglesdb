-- case: pgq_explain_delete
-- title: explain delete
-- owner: E1
-- expect: compatible
-- source: Differences: EXPLAIN ANALYZE; EXPLAIN of data changes
CREATE TABLE r0c_pgq_explain_delete (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_explain_delete VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
EXPLAIN (COSTS OFF) DELETE FROM r0c_pgq_explain_delete WHERE id=1;
