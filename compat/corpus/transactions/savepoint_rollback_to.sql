-- case: savepoint_rollback_to
-- title: Roll back changes after a savepoint
-- owner: unowned
-- expect: compatible

CREATE TABLE r0c_savepoint_rollback_to_t (id integer PRIMARY KEY);
BEGIN;
INSERT INTO r0c_savepoint_rollback_to_t VALUES (1);
SAVEPOINT first_row;
INSERT INTO r0c_savepoint_rollback_to_t VALUES (2);
ROLLBACK TO SAVEPOINT first_row;
COMMIT;
SELECT id FROM r0c_savepoint_rollback_to_t ORDER BY id;
