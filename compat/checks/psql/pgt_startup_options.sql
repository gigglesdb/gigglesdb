-- id: pgt_startup_options
-- title: Startup pgt_startup_options
-- owner: D
-- env: PGOPTIONS=-c work_mem=16MB -c default_transaction_isolation=repeatable\ read
SELECT current_setting('work_mem'), current_setting('default_transaction_isolation');
