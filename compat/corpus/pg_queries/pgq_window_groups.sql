-- case: pgq_window_groups
-- title: window groups
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions
CREATE TABLE r0c_pgq_window_groups (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_groups VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, sum(amount) OVER (ORDER BY amount GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS metric FROM r0c_pgq_window_groups ORDER BY id;
