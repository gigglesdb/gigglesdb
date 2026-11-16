-- case: pgq_window_count
-- title: window count
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions
CREATE TABLE r0c_pgq_window_count (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_count VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, count(amount) OVER (PARTITION BY dept) AS metric FROM r0c_pgq_window_count ORDER BY id;
