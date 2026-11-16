-- case: pgq_select_tablesample
-- title: select tablesample
-- owner: E3
-- expect: compatible
-- source: PostgreSQL 18 SELECT TABLESAMPLE REPEATABLE
CREATE TABLE r0c_pgq_select_tablesample (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_select_tablesample VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
SELECT id FROM r0c_pgq_select_tablesample TABLESAMPLE SYSTEM (100) REPEATABLE (42) ORDER BY id;
