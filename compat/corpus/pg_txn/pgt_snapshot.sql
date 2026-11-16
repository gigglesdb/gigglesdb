-- case: pgt_snapshot
-- title: Invalid imported snapshot is rejected
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Invalid imported snapshot is rejected
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- @error
SET TRANSACTION SNAPSHOT '00000000-00000000-0';
ROLLBACK;
SELECT pg_export_snapshot() IS NOT NULL AS exported;
