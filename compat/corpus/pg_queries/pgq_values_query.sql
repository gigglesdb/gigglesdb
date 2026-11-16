-- case: pgq_values_query
-- title: values query
-- owner: E3
-- expect: compatible
-- source: Differences: values query; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_values_query (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_values_query VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT * FROM (VALUES (2,'second'),(1,'first')) AS v(id,label) ORDER BY id;
