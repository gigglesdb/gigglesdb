-- case: pgq_window_ntile
-- title: window ntile
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions
CREATE TABLE r0c_pgq_window_ntile (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_ntile VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, ntile(3) OVER (ORDER BY amount NULLS LAST,id) AS metric FROM r0c_pgq_window_ntile ORDER BY id;
