-- case: pgq_merge_returning
-- title: merge returning
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 MERGE RETURNING
CREATE TABLE r0c_pgq_merge_returning (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_merge_returning VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
WITH changes AS (MERGE INTO r0c_pgq_merge_returning t USING (VALUES (1,'a',100),(5,'c',50)) s(id,dept,amount) ON t.id=s.id WHEN MATCHED THEN UPDATE SET amount=s.amount WHEN NOT MATCHED THEN INSERT (id,dept,amount) VALUES (s.id,s.dept,s.amount) RETURNING merge_action() AS action,t.id,t.amount) SELECT * FROM changes ORDER BY id;
