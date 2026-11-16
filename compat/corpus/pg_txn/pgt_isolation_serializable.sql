-- case: pgt_isolation_serializable
-- title: Isolation serializable
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Isolation serializable
BEGIN ISOLATION LEVEL SERIALIZABLE;
SHOW transaction_isolation;
SHOW transaction_read_only;
SHOW transaction_deferrable;
COMMIT;
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SHOW transaction_isolation;
ROLLBACK;
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SHOW default_transaction_isolation;
BEGIN;
SHOW transaction_isolation;
END;
RESET default_transaction_isolation;
