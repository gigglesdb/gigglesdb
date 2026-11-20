-- case: pgq_copy_options_header_true
-- title: COPY options header_true
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_header_true (id int, a text, b text);
COPY t_pgq_copy_options_header_true FROM PROGRAM 'echo skip,anything,here&& echo 1,x,z' WITH (FORMAT csv, HEADER true);
SELECT * FROM t_pgq_copy_options_header_true ORDER BY id;
