-- id: transaction_commit
-- title: Commit an inserted row
-- owner: D
CREATE TABLE r0_transaction_commit (id integer PRIMARY KEY, name text);
BEGIN;
INSERT INTO r0_transaction_commit (id, name) VALUES (1, 'a');
COMMIT;
SELECT id, name FROM r0_transaction_commit ORDER BY id;
