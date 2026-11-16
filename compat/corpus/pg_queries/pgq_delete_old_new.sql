-- case: pgq_delete_old_new
-- title: delete old new
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 SELECT / DML; Differences: window functions and data changes
CREATE TABLE r0c_pgq_delete_old_new (id int PRIMARY KEY, dept text, amount int);
INSERT INTO r0c_pgq_delete_old_new VALUES (1,'a',10),(2,'a',20),(3,'b',20),(4,'b',NULL);
DELETE FROM r0c_pgq_delete_old_new WHERE id=4 RETURNING WITH (OLD AS o, NEW AS n) o.id,n.id;
