-- case: pgt_deferrable
-- title: Serializable read only deferrable settings
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Serializable read only deferrable settings
START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE;
SHOW transaction_isolation;
SHOW transaction_read_only;
SHOW transaction_deferrable;
END;
BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;
SHOW transaction_deferrable;
SET TRANSACTION READ WRITE, NOT DEFERRABLE;
SHOW transaction_read_only;
SHOW transaction_deferrable;
ROLLBACK;
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;
SHOW default_transaction_deferrable;
BEGIN;
SHOW transaction_isolation;
SHOW transaction_read_only;
SHOW transaction_deferrable;
ROLLBACK;
RESET default_transaction_isolation;
RESET default_transaction_read_only;
RESET default_transaction_deferrable;
