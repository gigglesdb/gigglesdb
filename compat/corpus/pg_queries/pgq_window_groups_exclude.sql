-- case: pgq_window_groups_exclude
-- title: window groups exclude
-- owner: E3
-- expect: compatible
-- source: PostgreSQL 18 SELECT / DML; Differences: window functions and data changes
CREATE TABLE r0c_pgq_window_groups_exclude (id int PRIMARY KEY, dept text, amount int);
INSERT INTO r0c_pgq_window_groups_exclude VALUES (1,'a',10),(2,'a',20),(3,'b',20),(4,'b',NULL);
SELECT id,sum(amount) OVER (ORDER BY amount GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM r0c_pgq_window_groups_exclude ORDER BY id;
