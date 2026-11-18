-- case: pgq_copy_server_file_errors
-- title: copy to and from server files reports PostgreSQL's file errors
-- owner: E1
-- expect: compatible
-- source: GD-0079 Q6 COPY server files (relative paths, missing files, directories)
CREATE TABLE r0c_pgq_copy_server_file_errors (id int PRIMARY KEY, t text);
INSERT INTO r0c_pgq_copy_server_file_errors VALUES (1,'a');
-- @error
COPY r0c_pgq_copy_server_file_errors TO 'r0c_pgq_copy_server_file_errors.txt';
-- @error
COPY (SELECT 1) TO 'r0c_pgq_copy_server_file_errors.txt';
-- @error
COPY r0c_pgq_copy_server_file_errors TO '/r0c_pgq_copy_server_file_errors/missing/out.txt';
-- @error
COPY r0c_pgq_copy_server_file_errors TO '/';
-- @error
COPY r0c_pgq_copy_server_file_errors FROM 'r0c_pgq_copy_server_file_errors_missing.txt';
-- @error
COPY r0c_pgq_copy_server_file_errors FROM '/r0c_pgq_copy_server_file_errors/missing.txt';
-- @error
COPY r0c_pgq_copy_server_file_errors FROM '/';
SELECT count(*) FROM r0c_pgq_copy_server_file_errors;
