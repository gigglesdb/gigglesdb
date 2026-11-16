-- case: pgq_window_rows
-- title: window rows
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions
CREATE TABLE r0c_pgq_window_rows (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_rows VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, sum(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS metric FROM r0c_pgq_window_rows ORDER BY id;
