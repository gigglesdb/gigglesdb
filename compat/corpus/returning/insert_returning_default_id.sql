-- case: insert_returning_default_id
-- title: Return a generated serial identifier
-- owner: E1
-- expect: compatible

CREATE TABLE r0c_insert_returning_default_id_t (id serial PRIMARY KEY, name text);
INSERT INTO r0c_insert_returning_default_id_t (name) VALUES ('Ada') RETURNING id;
SELECT id, name FROM r0c_insert_returning_default_id_t ORDER BY id;
