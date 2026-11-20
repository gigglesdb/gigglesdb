-- case: pgq_copy_reject_limit
-- title: COPY runtime pgq_copy_reject_limit
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_reject_limit (a int PRIMARY KEY);
-- @error
COPY t_pgq_copy_reject_limit FROM PROGRAM 'echo 1&& echo bad&& echo bad' WITH (FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 1);
