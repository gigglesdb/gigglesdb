-- id: insert
-- title: Insert individual rows
-- owner: E1
CREATE TABLE r0_insert (id integer PRIMARY KEY, name text);
INSERT INTO r0_insert (id, name) VALUES (1, 'a');
INSERT INTO r0_insert (id, name) VALUES (2, 'b');
SELECT id, name FROM r0_insert ORDER BY id;
