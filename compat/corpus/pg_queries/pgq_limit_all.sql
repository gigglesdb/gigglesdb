-- case: pgq_limit_all
-- title: limit all
-- owner: E3
-- expect: compatible
-- source: Differences: limit all; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_limit_all (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_limit_all VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id FROM r0c_pgq_limit_all ORDER BY id LIMIT ALL OFFSET 2;
