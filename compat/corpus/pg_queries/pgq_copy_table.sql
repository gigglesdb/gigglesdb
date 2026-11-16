-- case: pgq_copy_table
-- title: copy table
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 COPY table TO STDOUT
CREATE TABLE r0c_pgq_copy_table (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_copy_table VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
DELETE FROM r0c_pgq_copy_table WHERE id<>1;
COPY r0c_pgq_copy_table (id,dept) TO STDOUT WITH (FORMAT CSV, HEADER);
