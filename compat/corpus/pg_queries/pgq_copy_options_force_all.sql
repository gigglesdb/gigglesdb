-- case: pgq_copy_options_force_all
-- title: COPY options force_all
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_options_force_all (id int, a text, b text);
COPY t_pgq_copy_options_force_all FROM PROGRAM 'echo id,a,b&& echo 1,,~~' WITH (FORMAT csv, HEADER, QUOTE '~', FORCE_NOT_NULL *, FORCE_NULL *, ON_ERROR stop, LOG_VERBOSITY default, ENCODING 'UTF8');
SELECT * FROM t_pgq_copy_options_force_all ORDER BY id;
