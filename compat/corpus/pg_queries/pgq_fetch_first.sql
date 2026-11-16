-- case: pgq_fetch_first
-- title: fetch first
-- owner: E3
-- expect: compatible
-- source: Differences: fetch first; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_fetch_first (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_fetch_first VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id FROM r0c_pgq_fetch_first ORDER BY id FETCH FIRST 2 ROWS ONLY;
