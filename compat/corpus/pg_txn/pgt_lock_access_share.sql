-- case: pgt_lock_access_share
-- title: LOCK ACCESS SHARE NOWAIT
-- owner: E1
-- expect: compatible
-- source: W1 LOCK every mode and NOWAIT
CREATE TABLE r0c_pgt_lock_access_share (id int PRIMARY KEY);
BEGIN;
LOCK TABLE r0c_pgt_lock_access_share IN ACCESS SHARE MODE NOWAIT;
SELECT mode, granted FROM pg_locks WHERE relation = 'r0c_pgt_lock_access_share'::regclass AND pid = pg_backend_pid() ORDER BY mode;
COMMIT;
