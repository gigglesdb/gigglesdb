-- case: pgq_literal_escape
-- title: literal escape
-- owner: E3
-- expect: compatible
-- source: Differences: literal escape; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_literal_escape (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_literal_escape VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id,E'it''s\tvalue\\ok' FROM r0c_pgq_literal_escape ORDER BY id;
