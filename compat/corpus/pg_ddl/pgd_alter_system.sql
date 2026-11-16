-- case: pgd_alter_system
-- title: alter system
-- owner: G
-- expect: compatible
-- source: W4 ALTER SYSTEM SET RESET
ALTER SYSTEM SET work_mem='8MB';
ALTER SYSTEM RESET work_mem;
SELECT count(*) FROM pg_file_settings WHERE name='work_mem';
