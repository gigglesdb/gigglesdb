-- id: transaction_rollback
-- title: Roll back an inserted row
-- owner: D
CREATE TABLE r0_transaction_rollback (id integer PRIMARY KEY, name text);
INSERT INTO r0_transaction_rollback (id, name) VALUES (1, 'a');
BEGIN;
INSERT INTO r0_transaction_rollback (id, name) VALUES (2, 'b');
ROLLBACK;
SELECT id, name FROM r0_transaction_rollback ORDER BY id;
