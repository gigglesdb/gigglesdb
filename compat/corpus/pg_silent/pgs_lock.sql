-- case: pgs_lock
-- title: LOCK TABLE is visible inside the transaction
-- owner: E1
-- expect: compatible
-- source: Differences: Read first / LOCK TABLE is visible inside the transaction
CREATE TABLE r0c_pgs_lock (id int PRIMARY KEY);
BEGIN;
LOCK TABLE r0c_pgs_lock IN ACCESS EXCLUSIVE MODE;
SELECT mode, granted FROM pg_locks WHERE relation = 'r0c_pgs_lock'::regclass AND pid = pg_backend_pid() ORDER BY mode;
ROLLBACK;
