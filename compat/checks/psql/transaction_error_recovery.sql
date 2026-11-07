-- id: transaction_error_recovery
-- title: Recover from an aborted transaction
-- owner: D
-- compare: stdout+stderr
\set VERBOSITY sqlstate
CREATE TABLE r0_transaction_error_recovery (id integer PRIMARY KEY);
INSERT INTO r0_transaction_error_recovery (id) VALUES (1);
\set ON_ERROR_STOP 0
BEGIN;
INSERT INTO r0_transaction_error_recovery (id) VALUES (1);
SELECT 1;
ROLLBACK;
\set ON_ERROR_STOP 1
SELECT count(*) FROM r0_transaction_error_recovery;
