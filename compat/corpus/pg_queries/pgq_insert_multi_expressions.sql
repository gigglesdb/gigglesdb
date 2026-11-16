-- case: pgq_insert_multi_expressions
-- title: insert multi expressions
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_insert_multi_expressions (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_insert_multi_expressions VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
INSERT INTO r0c_pgq_insert_multi_expressions VALUES (5,upper('c'),2::int+3,NULL,'a'||'b'),(6,lower('D'),4*2,NULL,'x'||'y');
SELECT * FROM r0c_pgq_insert_multi_expressions WHERE id>=5 ORDER BY id;
