-- case: pgq_insert_multi_concat
-- title: insert multi concat
-- owner: E1
-- expect: compatible
-- source: Differences: expressions in multi-row VALUES (concat)
CREATE TABLE r0c_pgq_insert_multi_concat (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_insert_multi_concat VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
INSERT INTO r0c_pgq_insert_multi_concat (id,label) VALUES (5,'a'||'b'),(6,'x'||'y');
SELECT id,label FROM r0c_pgq_insert_multi_concat WHERE id>=5 ORDER BY id;
