-- case: pgq_copy_default_no_default
-- title: COPY runtime pgq_copy_default_no_default
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_default_no_default (a int);
-- @error
COPY t_pgq_copy_default_no_default FROM PROGRAM 'echo D' WITH (FORMAT csv, DEFAULT 'D');
