-- case: pgq_window_named
-- title: window named
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions; named WINDOW
CREATE TABLE r0c_pgq_window_named (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_named VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, row_number() OVER w, sum(amount) OVER (w ROWS UNBOUNDED PRECEDING) FROM r0c_pgq_window_named WINDOW w AS (PARTITION BY dept ORDER BY id) ORDER BY id;
