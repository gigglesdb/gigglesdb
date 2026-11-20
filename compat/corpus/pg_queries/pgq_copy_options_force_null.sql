-- case: pgq_copy_options_force_null
-- title: COPY options force_null
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_force_null (id int PRIMARY KEY, a text, b text);
COPY t_pgq_copy_options_force_null FROM PROGRAM 'echo id,a,b&& echo 1,,~~' WITH (FORMAT csv, HEADER, QUOTE '~', FORCE_NOT_NULL (a), FORCE_NULL (b));
SELECT * FROM t_pgq_copy_options_force_null ORDER BY id;
