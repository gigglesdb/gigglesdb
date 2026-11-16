-- case: pgq_insert_multi_cast
-- title: insert multi cast
-- owner: E1
-- expect: compatible
-- source: Differences: expressions in multi-row VALUES (cast)
CREATE TABLE r0c_pgq_insert_multi_cast (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_insert_multi_cast VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
INSERT INTO r0c_pgq_insert_multi_cast (id,label) VALUES (5,2::int::text),(6,3::int::text);
SELECT id,label FROM r0c_pgq_insert_multi_cast WHERE id>=5 ORDER BY id;
