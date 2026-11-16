-- case: pgq_order_nulls_last
-- title: order nulls last
-- owner: E3
-- expect: compatible
-- source: Differences: order nulls last; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_order_nulls_last (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_order_nulls_last VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,amount FROM r0c_pgq_order_nulls_last ORDER BY amount ASC NULLS LAST,id;
