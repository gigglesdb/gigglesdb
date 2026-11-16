-- case: pgt_isolation_read_uncommitted
-- title: Isolation read uncommitted
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Isolation read uncommitted
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
SHOW transaction_isolation;
SHOW transaction_read_only;
SHOW transaction_deferrable;
COMMIT;
BEGIN;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SHOW transaction_isolation;
ROLLBACK;
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SHOW default_transaction_isolation;
BEGIN;
SHOW transaction_isolation;
END;
RESET default_transaction_isolation;
