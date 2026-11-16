-- case: pgq_copy_header
-- title: copy header
-- owner: E1
-- expect: compatible
-- source: Differences: COPY options; PostgreSQL 18 COPY TO STDOUT
CREATE TABLE r0c_pgq_copy_header (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_copy_header VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
COPY (SELECT id,dept,amount,label FROM r0c_pgq_copy_header ORDER BY id) TO STDOUT WITH (FORMAT CSV, HEADER);
