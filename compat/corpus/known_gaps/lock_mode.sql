-- case: lock_mode
-- title: LOCK TABLE in ACCESS EXCLUSIVE mode inside a transaction
-- owner: E2
-- expect: compatible
CREATE TABLE r0c_lock_mode (id int PRIMARY KEY);
BEGIN;
LOCK TABLE r0c_lock_mode IN ACCESS EXCLUSIVE MODE;
ROLLBACK;
