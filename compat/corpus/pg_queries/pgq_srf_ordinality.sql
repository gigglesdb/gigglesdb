-- case: pgq_srf_ordinality
-- title: srf ordinality
-- owner: E3
-- expect: compatible
-- source: PostgreSQL 18 SELECT / DML; Differences: window functions and data changes
CREATE TABLE r0c_pgq_srf_ordinality (id int PRIMARY KEY, dept text, amount int);
INSERT INTO r0c_pgq_srf_ordinality VALUES (1,'a',10),(2,'a',20),(3,'b',20),(4,'b',NULL);
SELECT t.id,u.value,u.position FROM r0c_pgq_srf_ordinality t CROSS JOIN LATERAL unnest(ARRAY[t.id,t.amount]) WITH ORDINALITY AS u(value,position) ORDER BY t.id,u.position;
