-- case: pgq_window_exclude_no_others
-- title: window exclude no others
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions; PostgreSQL 18 frame exclusion
CREATE TABLE r0c_pgq_window_exclude_no_others (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_exclude_no_others VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, sum(amount) OVER (ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) FROM r0c_pgq_window_exclude_no_others ORDER BY id;
