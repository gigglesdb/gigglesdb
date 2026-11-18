-- case: pgq_copy_server_unprivileged
-- title: copy to and from server files and programs needs superuser or a pg_*_server_* role
-- owner: E1
-- expect: compatible
-- source: GD-0079 Q6 COPY privilege checks (42501)
CREATE TABLE r0c_pgq_copy_server_unprivileged (id int PRIMARY KEY, t text);
INSERT INTO r0c_pgq_copy_server_unprivileged VALUES (1,'a');
CREATE ROLE r0c_pgq_copy_server_unprivileged_u;
GRANT ALL ON r0c_pgq_copy_server_unprivileged TO r0c_pgq_copy_server_unprivileged_u;
SET ROLE r0c_pgq_copy_server_unprivileged_u;
-- @error
COPY r0c_pgq_copy_server_unprivileged TO '/r0c_pgq_copy_server_unprivileged.txt';
-- @error
COPY r0c_pgq_copy_server_unprivileged TO 'r0c_pgq_copy_server_unprivileged.txt';
-- @error
COPY r0c_pgq_copy_server_unprivileged FROM 'r0c_pgq_copy_server_unprivileged.txt';
-- @error
COPY r0c_pgq_copy_server_unprivileged TO PROGRAM 'exit 0';
-- @error
COPY r0c_pgq_copy_server_unprivileged FROM PROGRAM 'exit 0';
-- @error
COPY (SELECT 1) TO '/r0c_pgq_copy_server_unprivileged.txt';
COPY r0c_pgq_copy_server_unprivileged TO STDOUT;
RESET ROLE;
DROP TABLE r0c_pgq_copy_server_unprivileged;
DROP ROLE r0c_pgq_copy_server_unprivileged_u;
