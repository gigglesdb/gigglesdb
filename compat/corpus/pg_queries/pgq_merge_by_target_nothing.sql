-- case: pgq_merge_by_target_nothing
-- title: merge by target nothing
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 MERGE WHEN NOT MATCHED BY TARGET
CREATE TABLE r0c_pgq_merge_by_target_nothing (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_merge_by_target_nothing VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
MERGE INTO r0c_pgq_merge_by_target_nothing t USING (VALUES (5)) s(id) ON t.id=s.id WHEN NOT MATCHED BY TARGET THEN DO NOTHING;
SELECT id FROM r0c_pgq_merge_by_target_nothing ORDER BY id;
