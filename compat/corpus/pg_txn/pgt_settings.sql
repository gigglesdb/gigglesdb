-- case: pgt_settings
-- title: Set local session reset and show settings
-- owner: D
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Set local session reset and show settings
SHOW work_mem;
SET work_mem = '16MB';
SHOW work_mem;
BEGIN;
SET LOCAL work_mem = '8MB';
SHOW work_mem;
COMMIT;
SHOW work_mem;
RESET work_mem;
SHOW work_mem;
SET SESSION statement_timeout = '5s';
SHOW statement_timeout;
RESET ALL;
SHOW statement_timeout;
