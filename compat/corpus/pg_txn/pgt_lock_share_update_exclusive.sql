-- case: pgt_lock_share_update_exclusive
-- title: LOCK SHARE UPDATE EXCLUSIVE NOWAIT
-- owner: E1
-- expect: compatible
-- source: W1 LOCK every mode and NOWAIT
CREATE TABLE r0c_pgt_lock_share_update_exclusive (id int PRIMARY KEY);
BEGIN;
LOCK TABLE r0c_pgt_lock_share_update_exclusive IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;
SELECT mode, granted FROM pg_locks WHERE relation = 'r0c_pgt_lock_share_update_exclusive'::regclass AND pid = pg_backend_pid() ORDER BY mode;
COMMIT;
