-- case: pgq_lock_of_table
-- title: lock of table
-- owner: E3
-- expect: compatible
-- source: PostgreSQL 18 SELECT locking OF
CREATE TABLE r0c_pgq_lock_of_table (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_lock_of_table VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
BEGIN;
SELECT a.id,b.id FROM r0c_pgq_lock_of_table a JOIN r0c_pgq_lock_of_table b ON b.parent=a.id ORDER BY a.id,b.id FOR UPDATE OF b NOWAIT;
COMMIT;
