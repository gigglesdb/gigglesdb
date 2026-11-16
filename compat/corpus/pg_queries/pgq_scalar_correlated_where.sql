-- case: pgq_scalar_correlated_where
-- title: scalar correlated where
-- owner: E3
-- expect: compatible
-- source: Differences: correlated scalar subqueries
CREATE TABLE r0c_pgq_scalar_correlated_where (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_scalar_correlated_where VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id FROM r0c_pgq_scalar_correlated_where t WHERE t.amount>(SELECT avg(s.amount) FROM r0c_pgq_scalar_correlated_where s WHERE s.dept=t.dept) ORDER BY t.id;
