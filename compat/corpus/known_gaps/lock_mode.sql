-- case: lock_mode
-- title: LOCK modes other than ACCESS SHARE
-- owner: E2
-- expect: intentional LOCK modes other than ACCESS SHARE
CREATE TABLE r0c_lock_mode (id int PRIMARY KEY);
BEGIN;
-- expect-sqlstate: 0A000
LOCK TABLE r0c_lock_mode IN ACCESS EXCLUSIVE MODE;
ROLLBACK;
