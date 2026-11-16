-- case: pgq_with_insert_returning
-- title: with insert returning
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_with_insert_returning (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_with_insert_returning VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH added AS (INSERT INTO r0c_pgq_with_insert_returning (id,dept) VALUES (5,'c'),(6,'d') RETURNING id,label) SELECT * FROM added ORDER BY id;
