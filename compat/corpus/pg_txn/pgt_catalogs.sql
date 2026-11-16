-- case: pgt_catalogs
-- title: Show all and session settings catalogs
-- owner: G
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Show all and session settings catalogs
-- Execute SHOW ALL without emitting environment-specific build strings or paths.
DO $$ BEGIN EXECUTE 'SHOW ALL'; END $$;
SELECT name, setting, unit FROM pg_settings WHERE name IN ('server_version_num','server_encoding','max_prepared_transactions') ORDER BY name;
SELECT state, backend_type FROM pg_stat_activity WHERE pid = pg_backend_pid();
