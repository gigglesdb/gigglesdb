-- case: pgq_merge_conditional
-- title: merge conditional
-- owner: E1
-- expect: compatible
-- source: Differences: MERGE; PostgreSQL 18 MERGE WHEN forms
CREATE TABLE r0c_pgq_merge_conditional (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_merge_conditional VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
MERGE INTO r0c_pgq_merge_conditional t USING (VALUES (1,'a',100),(5,'c',50)) s(id,dept,amount) ON t.id=s.id WHEN MATCHED AND t.amount<50 THEN UPDATE SET amount=s.amount WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND s.amount>0 THEN INSERT (id,dept,amount) VALUES (s.id,s.dept,s.amount) WHEN NOT MATCHED THEN DO NOTHING;
SELECT id,dept,amount,label FROM r0c_pgq_merge_conditional ORDER BY id;
