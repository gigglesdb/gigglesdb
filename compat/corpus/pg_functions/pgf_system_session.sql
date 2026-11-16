-- case: pgf_system_session
-- title: Session information and setting properties
-- owner: DD-0068
-- expect: compatible
-- source: functions.txt System and catalog functions; https://www.postgresql.org/docs/18/functions-info.html
SELECT pg_typeof(1);
SELECT pg_typeof(NULL::text);
SELECT current_database() IS NOT NULL;
SELECT current_schema();
SELECT current_schemas(false);
SELECT current_schemas(true);
SELECT current_catalog=current_database();
SELECT current_setting('TimeZone');
SELECT current_setting('DateStyle');
SELECT current_setting('r0c.missing',true);
SELECT current_user;
SELECT session_user;
SELECT current_role;
SELECT user;
BEGIN;
SELECT set_config('TimeZone','UTC',true);
SELECT current_setting('TimeZone');
SELECT current_setting('server_encoding');
ROLLBACK;
SELECT pg_backend_pid()>0;
SELECT txid_current()>0;
SELECT pg_current_xact_id() IS NOT NULL;
SELECT pg_is_in_recovery();
SELECT pg_postmaster_start_time() IS NOT NULL;
SELECT inet_server_addr() IS NULL;
SELECT v LIKE 'PostgreSQL %' FROM (VALUES (version())) AS input(v);
SELECT split_part('PostgreSQL 18.6',' ',1);
SELECT pg_sleep(0);
SELECT pg_column_size(1)>0;
SELECT format_type(23,NULL);
SELECT to_regclass('r0c_pgf_system_session_missing') IS NULL;
SELECT to_regtype('integer')::text;
SELECT has_database_privilege(d,'CONNECT') FROM (VALUES (current_database())) AS input(d);
