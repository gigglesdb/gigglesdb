-- case: pgq_copy_options_quote
-- title: COPY quote
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_quote (id int, a text, b text);
COPY t_pgq_copy_options_quote FROM PROGRAM 'echo 1,~x,y~,z' WITH (FORMAT csv, QUOTE '~');
SELECT * FROM t_pgq_copy_options_quote ORDER BY id;
