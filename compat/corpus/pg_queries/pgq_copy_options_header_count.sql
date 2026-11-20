-- case: pgq_copy_options_header_count
-- title: COPY options header_count
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_header_count (id int, a text, b text);
-- @error
COPY t_pgq_copy_options_header_count FROM PROGRAM 'echo id,a' WITH (FORMAT csv, HEADER MATCH);
SELECT * FROM t_pgq_copy_options_header_count ORDER BY id;
