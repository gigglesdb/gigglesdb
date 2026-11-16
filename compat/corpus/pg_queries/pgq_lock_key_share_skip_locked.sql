-- case: pgq_lock_key_share_skip_locked
-- title: lock key share skip locked
-- owner: E3
-- expect: compatible
-- source: Differences: FOR UPDATE, FOR SHARE and other row locks
CREATE TABLE r0c_pgq_lock_key_share_skip_locked (id int PRIMARY KEY, dept text, amount int, parent int, label text DEFAULT 'new');
INSERT INTO r0c_pgq_lock_key_share_skip_locked VALUES (1,'a',10,NULL,'root'),(2,'a',20,1,'child'),(3,'b',20,1,'leaf'),(4,'b',NULL,2,'empty');
BEGIN;
SELECT id FROM r0c_pgq_lock_key_share_skip_locked ORDER BY id LIMIT 2 FOR KEY SHARE SKIP LOCKED;
COMMIT;
