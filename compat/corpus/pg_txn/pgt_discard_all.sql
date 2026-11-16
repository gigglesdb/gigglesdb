-- case: pgt_discard_all
-- title: Discard all resets pooled session state
-- owner: D
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Discard all resets pooled session state
SET work_mem = '16MB';
PREPARE r0c_pgt_discard_all AS SELECT 1;
DISCARD ALL;
SHOW work_mem;
SELECT count(*) FROM pg_prepared_statements WHERE name = 'r0c_pgt_discard_all';
