-- case: pgq_copy_freeze_errors
-- title: COPY FREEZE rejected transaction activity
-- owner: E1
-- expect: compatible
CREATE TABLE t_pgq_copy_freeze_errors(i int PRIMARY KEY);
-- SQLSTATE 55000
-- @error
COPY t_pgq_copy_freeze_errors FROM PROGRAM 'echo 1' WITH (FREEZE);
ROLLBACK;
BEGIN;
CREATE TABLE t_pgq_copy_freeze_errors_parent(i int PRIMARY KEY);
SAVEPOINT child;
-- SQLSTATE 55000
-- @error
COPY t_pgq_copy_freeze_errors_parent FROM PROGRAM 'echo 2' WITH (FREEZE);
ROLLBACK;
BEGIN;
TRUNCATE t_pgq_copy_freeze_errors;
DECLARE freeze_c CURSOR FOR SELECT 1;
-- SQLSTATE 25000
-- @error
COPY t_pgq_copy_freeze_errors FROM PROGRAM 'echo 3' WITH (FREEZE);
ROLLBACK;
