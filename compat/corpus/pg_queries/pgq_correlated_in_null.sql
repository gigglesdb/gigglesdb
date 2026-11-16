-- case: pgq_correlated_in_null
-- title: correlated in null
-- owner: E3
-- expect: compatible
-- source: Differences: Scalar subqueries, correlated or not; Correlated IN (SELECT ...)
CREATE TABLE r0c_pgq_correlated_in_null (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_correlated_in_null VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id,t.amount IN (SELECT s.amount FROM r0c_pgq_correlated_in_null s WHERE s.dept=t.dept) FROM r0c_pgq_correlated_in_null t ORDER BY t.id;
