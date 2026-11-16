-- case: pgq_fetch_ties
-- title: fetch ties
-- owner: E3
-- expect: compatible
-- source: Differences: fetch ties; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_fetch_ties (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_fetch_ties VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,amount FROM (SELECT id,amount FROM r0c_pgq_fetch_ties ORDER BY amount FETCH FIRST 2 ROWS WITH TIES) s ORDER BY amount,id;
