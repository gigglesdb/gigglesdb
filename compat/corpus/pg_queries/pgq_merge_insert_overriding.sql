-- case: pgq_merge_insert_overriding
-- title: merge insert overriding
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 MERGE INSERT OVERRIDING
CREATE TABLE r0c_pgq_merge_insert_overriding (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_merge_insert_overriding VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
CREATE TABLE r0c_pgq_merge_insert_overriding_identity (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY);
MERGE INTO r0c_pgq_merge_insert_overriding_identity t USING (VALUES (50)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT (id) OVERRIDING SYSTEM VALUE VALUES (s.id) RETURNING t.id;
