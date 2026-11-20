-- case: pgq_copy_on_error_constraint
-- title: COPY runtime pgq_copy_on_error_constraint
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_on_error_constraint (a int NOT NULL);
-- @error
COPY t_pgq_copy_on_error_constraint FROM PROGRAM 'echo 1&& echo N' WITH (FORMAT csv, NULL 'N', ON_ERROR ignore);
