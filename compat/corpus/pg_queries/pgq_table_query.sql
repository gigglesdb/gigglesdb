-- case: pgq_table_query
-- title: table query
-- owner: E3
-- expect: compatible
-- source: Differences: table query; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_table_query (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_table_query VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
TABLE r0c_pgq_table_query ORDER BY id;
