-- case: savepoint_release
-- title: Release a savepoint and commit its changes
-- owner: unowned
-- expect: compatible

CREATE TABLE r0c_savepoint_release_t (id integer PRIMARY KEY);
BEGIN;
SAVEPOINT pending_rows;
INSERT INTO r0c_savepoint_release_t VALUES (1);
INSERT INTO r0c_savepoint_release_t VALUES (2);
RELEASE SAVEPOINT pending_rows;
COMMIT;
SELECT id FROM r0c_savepoint_release_t ORDER BY id;
