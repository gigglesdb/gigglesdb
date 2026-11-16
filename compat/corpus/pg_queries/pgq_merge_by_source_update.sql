-- case: pgq_merge_by_source_update
-- title: merge by source update
-- owner: E1
-- expect: compatible
-- source: Differences: MERGE; PostgreSQL 18 MERGE WHEN forms
CREATE TABLE r0c_pgq_merge_by_source_update (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_merge_by_source_update VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
MERGE INTO r0c_pgq_merge_by_source_update t USING (VALUES (1,'a',100),(5,'c',50)) s(id,dept,amount) ON t.id=s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET label='stale';
SELECT id,dept,amount,label FROM r0c_pgq_merge_by_source_update ORDER BY id;
