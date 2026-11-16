-- case: pgt_chain
-- title: Commit and rollback and chain preserve characteristics
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Commit and rollback and chain preserve characteristics
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
COMMIT AND CHAIN;
SHOW transaction_isolation;
SHOW transaction_read_only;
ROLLBACK AND CHAIN;
SHOW transaction_isolation;
SHOW transaction_read_only;
COMMIT AND NO CHAIN;
