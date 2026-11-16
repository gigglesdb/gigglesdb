-- case: pgs_isolation
-- title: Transaction isolation reporting
-- owner: E1
-- expect: compatible
-- source: Differences: Read first / Transaction isolation reporting
SHOW transaction_isolation;
SHOW default_transaction_isolation;
BEGIN ISOLATION LEVEL REPEATABLE READ;
SHOW transaction_isolation;
COMMIT;
BEGIN ISOLATION LEVEL SERIALIZABLE;
SHOW transaction_isolation;
ROLLBACK;
