-- case: pgq_upsert_returning
-- title: upsert returning
-- owner: E1
-- expect: compatible
-- source: Differences: INSERT trailing columns; expressions in multi-row VALUES; UPDATE FROM/row assignment; DELETE USING; data-modifying WITH; PostgreSQL 18 INSERT/UPDATE/DELETE
CREATE TABLE r0c_pgq_upsert_returning (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_upsert_returning VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH saved AS (INSERT INTO r0c_pgq_upsert_returning (id,dept,amount) VALUES (1,'a',5),(5,'c',15) ON CONFLICT (id) DO UPDATE SET amount=r0c_pgq_upsert_returning.amount+EXCLUDED.amount RETURNING id,amount) SELECT * FROM saved ORDER BY id;
