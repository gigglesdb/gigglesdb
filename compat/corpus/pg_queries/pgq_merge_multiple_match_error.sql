-- case: pgq_merge_multiple_match_error
-- title: merge multiple match error
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 MERGE cardinality violation
CREATE TABLE r0c_pgq_merge_multiple_match_error (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_merge_multiple_match_error VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
-- @error
MERGE INTO r0c_pgq_merge_multiple_match_error t USING (VALUES (1,10),(1,20)) s(id,amount) ON t.id=s.id WHEN MATCHED THEN UPDATE SET amount=s.amount;
