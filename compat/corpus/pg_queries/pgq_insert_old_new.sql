-- case: pgq_insert_old_new
-- title: insert old new
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 SELECT / DML; Differences: window functions and data changes
CREATE TABLE r0c_pgq_insert_old_new (id int PRIMARY KEY, dept text, amount int);
INSERT INTO r0c_pgq_insert_old_new VALUES (1,'a',10),(2,'a',20),(3,'b',20),(4,'b',NULL);
INSERT INTO r0c_pgq_insert_old_new VALUES (5,'c',30) RETURNING WITH (OLD AS o, NEW AS n) o.id,n.id,n.amount;
