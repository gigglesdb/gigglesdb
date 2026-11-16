-- case: pgq_exists
-- title: exists
-- owner: E3
-- expect: compatible
-- source: Differences: Scalar subqueries, correlated or not; Correlated IN (SELECT ...)
CREATE TABLE r0c_pgq_exists (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_exists VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id FROM r0c_pgq_exists t WHERE EXISTS (SELECT 1 FROM r0c_pgq_exists s WHERE s.parent=t.id) ORDER BY t.id;
