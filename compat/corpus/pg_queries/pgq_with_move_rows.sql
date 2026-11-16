-- case: pgq_with_move_rows
-- title: with move rows
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_with_move_rows (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_with_move_rows VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
CREATE TABLE r0c_pgq_with_move_rows_archive (id int PRIMARY KEY, label text);
WITH removed AS (DELETE FROM r0c_pgq_with_move_rows WHERE id>=3 RETURNING id,label), archived AS (INSERT INTO r0c_pgq_with_move_rows_archive SELECT * FROM removed RETURNING id,label) SELECT * FROM archived ORDER BY id;
