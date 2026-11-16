-- case: pgq_merge_not_matched_target
-- title: merge not matched target
-- owner: E1
-- expect: compatible
-- source: Differences: MERGE; PostgreSQL 18 MERGE WHEN forms
CREATE TABLE r0c_pgq_merge_not_matched_target (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_merge_not_matched_target VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
MERGE INTO r0c_pgq_merge_not_matched_target t USING (VALUES (1,'a',100),(5,'c',50)) s(id,dept,amount) ON t.id=s.id WHEN NOT MATCHED BY TARGET THEN INSERT (id,dept,amount) VALUES (s.id,s.dept,s.amount);
SELECT id,dept,amount,label FROM r0c_pgq_merge_not_matched_target ORDER BY id;
