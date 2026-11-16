-- case: pgq_limit_expression
-- title: limit expression
-- owner: E3
-- expect: compatible
-- source: Differences: limit expression; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_limit_expression (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_limit_expression VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id FROM r0c_pgq_limit_expression ORDER BY id LIMIT (1+1) OFFSET (2-1);
