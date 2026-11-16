-- case: pgq_window_lead
-- title: window lead
-- owner: E3
-- expect: compatible
-- source: Differences: Window functions
CREATE TABLE r0c_pgq_window_lead (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_window_lead VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id, lead(amount,1,-1) OVER (PARTITION BY dept ORDER BY id) AS metric FROM r0c_pgq_window_lead ORDER BY id;
