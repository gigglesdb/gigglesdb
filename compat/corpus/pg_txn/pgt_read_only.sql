-- case: pgt_read_only
-- title: Read only transactions reject writes
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Read only transactions reject writes
CREATE TABLE r0c_pgt_read_only (id int PRIMARY KEY);
BEGIN;
SET TRANSACTION READ ONLY;
SHOW transaction_read_only;
-- @error
INSERT INTO r0c_pgt_read_only VALUES (1);
ROLLBACK;
SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;
BEGIN;
SHOW transaction_read_only;
-- @error
UPDATE r0c_pgt_read_only SET id = 2;
ROLLBACK;
SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;
BEGIN READ WRITE;
INSERT INTO r0c_pgt_read_only VALUES (1);
COMMIT;
SELECT id FROM r0c_pgt_read_only ORDER BY id;
