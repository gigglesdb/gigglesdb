-- case: pgq_isnull
-- title: isnull
-- owner: E3
-- expect: compatible
-- source: Differences: isnull; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_isnull (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_isnull VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id FROM r0c_pgq_isnull WHERE amount ISNULL ORDER BY id;
