-- case: pgq_copy_server_roles
-- title: pg_read_server_files, pg_write_server_files and pg_execute_server_program allow COPY
-- owner: E1
-- expect: compatible
-- source: GD-0079 Q6 COPY predefined roles
CREATE TABLE r0c_pgq_copy_server_roles (id int PRIMARY KEY, t text);
INSERT INTO r0c_pgq_copy_server_roles VALUES (1,'a');
CREATE ROLE r0c_pgq_copy_server_roles_r IN ROLE pg_read_server_files;
CREATE ROLE r0c_pgq_copy_server_roles_w;
GRANT pg_write_server_files TO r0c_pgq_copy_server_roles_w;
CREATE ROLE r0c_pgq_copy_server_roles_p IN ROLE pg_execute_server_program;
CREATE ROLE r0c_pgq_copy_server_roles_n NOINHERIT IN ROLE pg_read_server_files;
CREATE ROLE r0c_pgq_copy_server_roles_c IN ROLE r0c_pgq_copy_server_roles_r;
GRANT ALL ON r0c_pgq_copy_server_roles TO PUBLIC;
SELECT rolname FROM pg_roles WHERE rolname IN ('pg_read_server_files', 'pg_write_server_files', 'pg_execute_server_program') ORDER BY rolname;
SET ROLE r0c_pgq_copy_server_roles_r;
-- @error
COPY r0c_pgq_copy_server_roles FROM 'r0c_pgq_copy_server_roles_missing.txt';
-- @error
COPY r0c_pgq_copy_server_roles TO '/r0c_pgq_copy_server_roles.txt';
-- @error
COPY r0c_pgq_copy_server_roles FROM PROGRAM 'exit 0';
RESET ROLE;
SET ROLE r0c_pgq_copy_server_roles_c;
-- @error
COPY r0c_pgq_copy_server_roles FROM 'r0c_pgq_copy_server_roles_missing.txt';
RESET ROLE;
SET ROLE r0c_pgq_copy_server_roles_w;
-- @error
COPY r0c_pgq_copy_server_roles TO 'r0c_pgq_copy_server_roles.txt';
-- @error
COPY r0c_pgq_copy_server_roles FROM 'r0c_pgq_copy_server_roles_missing.txt';
RESET ROLE;
SET ROLE r0c_pgq_copy_server_roles_p;
COPY r0c_pgq_copy_server_roles FROM PROGRAM 'exit 0';
-- @error
COPY r0c_pgq_copy_server_roles FROM 'r0c_pgq_copy_server_roles_missing.txt';
RESET ROLE;
SET ROLE r0c_pgq_copy_server_roles_n;
-- @error
COPY r0c_pgq_copy_server_roles FROM 'r0c_pgq_copy_server_roles_missing.txt';
RESET ROLE;
DROP TABLE r0c_pgq_copy_server_roles;
DROP ROLE r0c_pgq_copy_server_roles_c;
DROP ROLE r0c_pgq_copy_server_roles_r;
DROP ROLE r0c_pgq_copy_server_roles_w;
DROP ROLE r0c_pgq_copy_server_roles_p;
DROP ROLE r0c_pgq_copy_server_roles_n;
