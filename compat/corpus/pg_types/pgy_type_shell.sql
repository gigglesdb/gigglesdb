-- case: pgy_type_shell
-- title: type shell
-- owner: E2
-- expect: compatible
-- source: W5 CREATE/ALTER/DROP TYPE; types.txt
CREATE TYPE r0c_pgy_type_shell_shell;
SELECT typname,typisdefined FROM pg_type WHERE typname='r0c_pgy_type_shell_shell' ORDER BY typname;
ALTER TYPE r0c_pgy_type_shell_shell OWNER TO postgres;
-- @error
ALTER TYPE r0c_pgy_type_shell_shell RENAME TO r0c_pgy_type_shell_new;
DROP TYPE r0c_pgy_type_shell_shell;
SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='r0c_pgy_type_shell_shell');
