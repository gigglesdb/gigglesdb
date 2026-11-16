-- case: pgq_update_old_new
-- title: update old new
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_update_old_new (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_update_old_new VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
UPDATE r0c_pgq_update_old_new SET amount=amount+5 WHERE id=1 RETURNING WITH (OLD AS o, NEW AS n) o.id,o.amount AS before,n.amount AS after;
