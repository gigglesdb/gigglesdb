-- case: pgq_copy_on_error_ignore
-- title: COPY runtime pgq_copy_on_error_ignore
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_on_error_ignore (a int);
COPY t_pgq_copy_on_error_ignore FROM PROGRAM 'echo 1&& echo bad&& echo 2' WITH (FORMAT csv, ON_ERROR ignore);
SELECT * FROM t_pgq_copy_on_error_ignore ORDER BY a;
