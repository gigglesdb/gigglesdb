-- case: delete_returning
-- title: Delete one row and return its old values
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_delete_returning_t (id integer PRIMARY KEY, name text);
INSERT INTO r0c_delete_returning_t VALUES (1, 'Ada');
INSERT INTO r0c_delete_returning_t VALUES (2, 'Bea');
DELETE FROM r0c_delete_returning_t WHERE id = 1 RETURNING id, name;
SELECT id, name FROM r0c_delete_returning_t ORDER BY id;
