-- case: update_returning
-- title: Update one row and return its new values
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_update_returning_t (id integer PRIMARY KEY, score integer);
INSERT INTO r0c_update_returning_t VALUES (1, 10);
INSERT INTO r0c_update_returning_t VALUES (2, 20);
UPDATE r0c_update_returning_t SET score = 15 WHERE id = 1 RETURNING id, score;
SELECT id, score FROM r0c_update_returning_t ORDER BY id;
