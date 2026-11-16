-- case: pgt_lock_share
-- title: LOCK SHARE NOWAIT
-- owner: E1
-- expect: compatible
-- source: W1 LOCK every mode and NOWAIT
CREATE TABLE r0c_pgt_lock_share (id int PRIMARY KEY);
BEGIN;
LOCK TABLE r0c_pgt_lock_share IN SHARE MODE NOWAIT;
SELECT mode, granted FROM pg_locks WHERE relation = 'r0c_pgt_lock_share'::regclass AND pid = pg_backend_pid() ORDER BY mode;
COMMIT;
