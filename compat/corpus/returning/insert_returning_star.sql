-- case: insert_returning_star
-- title: Insert and return all columns
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_insert_returning_star_t (id integer, name text);
INSERT INTO r0c_insert_returning_star_t VALUES (1, 'Ada') RETURNING *;
SELECT id, name FROM r0c_insert_returning_star_t ORDER BY id;
