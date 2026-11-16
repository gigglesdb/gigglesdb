-- case: pgq_lock_no_key_update_nowait
-- title: lock no key update nowait
-- owner: E3
-- expect: compatible
-- source: Differences: FOR UPDATE, FOR SHARE and other row locks
CREATE TABLE r0c_pgq_lock_no_key_update_nowait (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_lock_no_key_update_nowait VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
BEGIN;
SELECT id FROM r0c_pgq_lock_no_key_update_nowait ORDER BY id LIMIT 2 FOR NO KEY UPDATE NOWAIT;
COMMIT;
