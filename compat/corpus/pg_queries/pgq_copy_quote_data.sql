-- case: pgq_copy_quote_data
-- title: copy quote data
-- owner: E1
-- expect: compatible
-- source: PostgreSQL 18 COPY CSV escaping
CREATE TABLE r0c_pgq_copy_quote_data (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_copy_quote_data VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
UPDATE r0c_pgq_copy_quote_data SET label='say "hello", friend' WHERE id=1;
COPY (SELECT id,label FROM r0c_pgq_copy_quote_data ORDER BY id) TO STDOUT WITH (FORMAT CSV, HEADER, FORCE_QUOTE (label));
