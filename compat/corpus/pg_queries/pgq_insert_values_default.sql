-- case: pgq_insert_values_default
-- title: insert values default
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 INSERT VALUES DEFAULT
CREATE TABLE r0c_pgq_insert_values_default (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_insert_values_default VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
INSERT INTO r0c_pgq_insert_values_default (id,label) VALUES (5,DEFAULT),(6,'custom');
SELECT id,label FROM r0c_pgq_insert_values_default WHERE id>=5 ORDER BY id;
