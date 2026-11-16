-- case: pgq_merge_old_new
-- title: merge old new
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 SELECT / DML; Differences: window functions and data changes
CREATE TABLE r0c_pgq_merge_old_new (id int PRIMARY KEY, dept text, amount int);
INSERT INTO r0c_pgq_merge_old_new VALUES (1,'a',10),(2,'a',20),(3,'b',20),(4,'b',NULL);
MERGE INTO r0c_pgq_merge_old_new t USING (VALUES (1,99)) s(id,amount) ON t.id=s.id WHEN MATCHED THEN UPDATE SET amount=s.amount RETURNING WITH (OLD AS o, NEW AS n) merge_action(),o.id,o.amount,n.amount;
