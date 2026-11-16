-- case: pgq_delete_using
-- title: delete using
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_delete_using (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_delete_using VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
DELETE FROM r0c_pgq_delete_using AS t USING (VALUES (1),(3)) AS obsolete(id) WHERE t.id=obsolete.id;
SELECT id FROM r0c_pgq_delete_using ORDER BY id;
