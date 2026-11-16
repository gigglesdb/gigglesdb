-- case: pgq_update_row_many_error
-- title: update row many error
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 SELECT / DML; Differences: window functions and data changes
CREATE TABLE r0c_pgq_update_row_many_error (id int PRIMARY KEY, dept text, amount int);
INSERT INTO r0c_pgq_update_row_many_error VALUES (1,'a',10),(2,'a',20),(3,'b',20),(4,'b',NULL);
-- @error
UPDATE r0c_pgq_update_row_many_error SET (dept,amount)=(SELECT dept,amount FROM r0c_pgq_update_row_many_error) WHERE id=1;
