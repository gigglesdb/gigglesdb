-- case: pgq_order_using
-- title: order using
-- owner: E3
-- expect: compatible
-- source: Differences: order using; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_order_using (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_order_using VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,amount FROM r0c_pgq_order_using ORDER BY amount USING > NULLS LAST,id;
