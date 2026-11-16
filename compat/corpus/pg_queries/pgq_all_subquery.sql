-- case: pgq_all_subquery
-- title: all subquery
-- owner: E3
-- expect: compatible
-- source: Differences: ANY, ALL, SOME
CREATE TABLE r0c_pgq_all_subquery (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_all_subquery VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT t.id,t.amount = ALL (SELECT s.amount FROM r0c_pgq_all_subquery s WHERE s.dept=t.dept) AS matches FROM r0c_pgq_all_subquery t ORDER BY t.id;
