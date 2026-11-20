-- case: pgq_copy_options_header_mismatch
-- title: COPY header_mismatch
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_header_mismatch (id int, a text, b text);
-- @error
COPY t_pgq_copy_options_header_mismatch FROM PROGRAM 'echo id,x,b' WITH (FORMAT csv, HEADER MATCH);
SELECT * FROM t_pgq_copy_options_header_mismatch ORDER BY id;
