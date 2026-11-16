-- case: pgq_window_rank
-- title: window rank
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions
CREATE TABLE r0c_pgq_window_rank (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_rank VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, rank() OVER (ORDER BY amount NULLS LAST) AS metric FROM r0c_pgq_window_rank ORDER BY id;
