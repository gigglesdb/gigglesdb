-- case: pgq_update_current_of
-- title: update current of
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_update_current_of (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_update_current_of VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
BEGIN;
DECLARE r0c_pgq_update_current_of_c CURSOR FOR SELECT * FROM r0c_pgq_update_current_of ORDER BY id FOR UPDATE;
FETCH FIRST FROM r0c_pgq_update_current_of_c;
UPDATE r0c_pgq_update_current_of SET amount=99 WHERE CURRENT OF r0c_pgq_update_current_of_c RETURNING id,amount;
CLOSE r0c_pgq_update_current_of_c;
COMMIT;
