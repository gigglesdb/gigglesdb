-- case: pgq_with_update_returning
-- title: with update returning
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_with_update_returning (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_with_update_returning VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH changed AS (UPDATE r0c_pgq_with_update_returning SET amount=amount+1 WHERE amount IS NOT NULL RETURNING id,amount) SELECT * FROM changed ORDER BY id;
