-- case: pgq_upsert_partial
-- title: upsert partial
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_upsert_partial (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_upsert_partial VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
CREATE UNIQUE INDEX r0c_pgq_upsert_partial_u ON r0c_pgq_upsert_partial (dept) WHERE parent IS NULL;
INSERT INTO r0c_pgq_upsert_partial (id,dept,amount) VALUES (5,'a',99) ON CONFLICT (dept) WHERE parent IS NULL DO UPDATE SET amount=EXCLUDED.amount RETURNING id,amount;
DROP INDEX r0c_pgq_upsert_partial_u;
