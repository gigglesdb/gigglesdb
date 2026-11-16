-- case: pgt_savepoints
-- title: Savepoint release and rollback recovery
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Savepoint release and rollback recovery
CREATE TABLE r0c_pgt_savepoints (id int PRIMARY KEY);
BEGIN;
INSERT INTO r0c_pgt_savepoints VALUES (1);
SAVEPOINT r0c_pgt_savepoints_a;
INSERT INTO r0c_pgt_savepoints VALUES (2);
SAVEPOINT r0c_pgt_savepoints_b;
-- @error
INSERT INTO r0c_pgt_savepoints VALUES (1);
ROLLBACK TO SAVEPOINT r0c_pgt_savepoints_b;
RELEASE SAVEPOINT r0c_pgt_savepoints_b;
ROLLBACK TO SAVEPOINT r0c_pgt_savepoints_a;
RELEASE SAVEPOINT r0c_pgt_savepoints_a;
COMMIT;
SELECT id FROM r0c_pgt_savepoints ORDER BY id;
