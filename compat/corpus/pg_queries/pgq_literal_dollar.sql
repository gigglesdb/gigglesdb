-- case: pgq_literal_dollar
-- title: literal dollar
-- owner: E3
-- expect: compatible
-- source: Differences: literal dollar; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_literal_dollar (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_literal_dollar VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,$tag$it's a label$tag$ FROM r0c_pgq_literal_dollar ORDER BY id;
