-- case: pgt_isolation_repeatable_read
-- title: Isolation repeatable read
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Isolation repeatable read
BEGIN ISOLATION LEVEL REPEATABLE READ;
SHOW transaction_isolation;
SHOW transaction_read_only;
SHOW transaction_deferrable;
COMMIT;
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SHOW transaction_isolation;
ROLLBACK;
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SHOW default_transaction_isolation;
BEGIN;
SHOW transaction_isolation;
END;
RESET default_transaction_isolation;
