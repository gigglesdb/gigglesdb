-- case: set_transaction_isolation
-- title: Set repeatable read isolation for a transaction
-- owner: D
-- expect: compatible

BEGIN ISOLATION LEVEL REPEATABLE READ;
SHOW transaction_isolation;
COMMIT;
