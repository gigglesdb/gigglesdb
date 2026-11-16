-- case: pgt_lock_access_exclusive
-- title: LOCK ACCESS EXCLUSIVE NOWAIT
-- owner: E1
-- expect: compatible
-- source: W1 LOCK every mode and NOWAIT
CREATE TABLE r0c_pgt_lock_access_exclusive (id int PRIMARY KEY);
BEGIN;
LOCK TABLE r0c_pgt_lock_access_exclusive IN ACCESS EXCLUSIVE MODE NOWAIT;
SELECT mode, granted FROM pg_locks WHERE relation = 'r0c_pgt_lock_access_exclusive'::regclass AND pid = pg_backend_pid() ORDER BY mode;
COMMIT;
