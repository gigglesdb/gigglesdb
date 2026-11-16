-- case: pgt_advisory_shared
-- title: Shared advisory session and transaction locks
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Shared advisory session and transaction locks
SELECT pg_advisory_lock_shared(918274661::bigint);
SELECT pg_try_advisory_lock_shared(918274661::bigint);
SELECT pg_advisory_unlock_shared(918274661::bigint);
SELECT pg_advisory_unlock_shared(918274661::bigint);
SELECT pg_advisory_lock_shared(918274,661);
SELECT pg_advisory_unlock_shared(918274,661);
BEGIN;
SELECT pg_advisory_xact_lock_shared(918274661::bigint);
SELECT pg_try_advisory_xact_lock_shared(918274661::bigint);
SELECT pg_advisory_xact_lock_shared(918274,661);
SELECT pg_try_advisory_xact_lock_shared(918274,661);
SELECT mode, granted, count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() GROUP BY mode, granted ORDER BY mode, granted;
COMMIT;
