-- case: pgq_literal_leading_dot
-- title: literal leading dot
-- owner: E3
-- expect: compatible
-- source: Differences: literal leading dot; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_literal_leading_dot (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_literal_leading_dot VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,amount*.5 FROM r0c_pgq_literal_leading_dot ORDER BY id;
