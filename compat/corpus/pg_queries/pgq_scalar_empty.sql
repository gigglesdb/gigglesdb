-- case: pgq_scalar_empty
-- title: scalar empty
-- owner: E3
-- expect: compatible
-- source: Differences: Scalar subqueries, correlated or not; Correlated IN (SELECT ...)
CREATE TABLE r0c_pgq_scalar_empty (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_scalar_empty VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,(SELECT amount FROM r0c_pgq_scalar_empty s WHERE s.id=99) FROM r0c_pgq_scalar_empty ORDER BY id;
