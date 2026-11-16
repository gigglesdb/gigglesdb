-- case: pgq_window_first_value
-- title: window first value
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions
CREATE TABLE r0c_pgq_window_first_value (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_first_value VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, first_value(amount) OVER (PARTITION BY dept ORDER BY id) AS metric FROM r0c_pgq_window_first_value ORDER BY id;
