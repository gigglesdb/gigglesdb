-- case: pgq_copy_freeze
-- title: COPY FREEZE current subtransaction preconditions
-- owner: E1
-- expect: compatible
BEGIN;
CREATE TABLE t_pgq_copy_freeze(i int);
COPY t_pgq_copy_freeze FROM PROGRAM 'echo 1' WITH (FREEZE);
COMMIT;
SELECT * FROM t_pgq_copy_freeze;
BEGIN;
TRUNCATE t_pgq_copy_freeze;
COPY t_pgq_copy_freeze FROM PROGRAM 'echo 2' FREEZE CSV;
COMMIT;
SELECT * FROM t_pgq_copy_freeze;
BEGIN;
SAVEPOINT child;
CREATE TABLE t_pgq_copy_freeze_release(i int);
RELEASE child;
COPY t_pgq_copy_freeze_release FROM PROGRAM 'echo 3' WITH (FREEZE true);
COMMIT;
SELECT * FROM t_pgq_copy_freeze_release;
