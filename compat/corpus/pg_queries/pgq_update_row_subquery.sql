-- case: pgq_update_row_subquery
-- title: update row subquery
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_update_row_subquery (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_update_row_subquery VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
UPDATE r0c_pgq_update_row_subquery t SET (dept,amount)=(SELECT s.dept,s.amount FROM r0c_pgq_update_row_subquery s WHERE s.id=t.parent) WHERE t.id=2 RETURNING t.id,t.dept,t.amount;
