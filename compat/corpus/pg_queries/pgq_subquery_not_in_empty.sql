-- case: pgq_subquery_not_in_empty
-- title: subquery not in empty
-- owner: E3
-- expect: compatible
-- source: Differences: correlated IN; PostgreSQL NULL and empty subqueries
CREATE TABLE r0c_pgq_subquery_not_in_empty (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_subquery_not_in_empty VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id,t.amount NOT IN (SELECT s.amount FROM r0c_pgq_subquery_not_in_empty s WHERE s.parent=t.id AND s.id<0) FROM r0c_pgq_subquery_not_in_empty t ORDER BY t.id;
