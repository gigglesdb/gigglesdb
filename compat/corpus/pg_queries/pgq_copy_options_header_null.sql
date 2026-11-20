-- case: pgq_copy_options_header_null
-- title: COPY options header_null
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_header_null (id int, a text, b text);
-- @error
COPY t_pgq_copy_options_header_null FROM PROGRAM 'echo id,a,b' WITH (FORMAT csv, HEADER MATCH, NULL 'a');
SELECT * FROM t_pgq_copy_options_header_null ORDER BY id;
