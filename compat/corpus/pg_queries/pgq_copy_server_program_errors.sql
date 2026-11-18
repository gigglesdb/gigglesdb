-- case: pgq_copy_server_program_errors
-- title: copy to and from a failing program reports program failure
-- owner: E1
-- expect: compatible
-- source: GD-0079 Q6 COPY PROGRAM non-zero exit
CREATE TABLE r0c_pgq_copy_server_program_errors (id int PRIMARY KEY, t text);
INSERT INTO r0c_pgq_copy_server_program_errors VALUES (1,'a');
-- @error
COPY r0c_pgq_copy_server_program_errors TO PROGRAM 'exit 3';
-- @error
COPY r0c_pgq_copy_server_program_errors FROM PROGRAM 'exit 4';
-- @error
COPY (SELECT 2, 'b') TO PROGRAM 'exit 5';
SELECT count(*) FROM r0c_pgq_copy_server_program_errors;
