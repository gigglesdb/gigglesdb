-- case: pgq_literal_bit
-- title: literal bit
-- owner: E3
-- expect: compatible
-- source: Differences: literal bit; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_literal_bit (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_literal_bit VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,B'1010' FROM r0c_pgq_literal_bit ORDER BY id;
