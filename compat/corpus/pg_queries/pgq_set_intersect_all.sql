-- case: pgq_set_intersect_all
-- title: set intersect all
-- owner: E3
-- expect: compatible
-- source: Differences: UNION, INTERSECT, EXCEPT
CREATE TABLE r0c_pgq_set_intersect_all (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_set_intersect_all VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT amount FROM r0c_pgq_set_intersect_all WHERE id<=3 INTERSECT ALL SELECT amount FROM r0c_pgq_set_intersect_all WHERE id>=2 ORDER BY amount NULLS LAST LIMIT 4;
