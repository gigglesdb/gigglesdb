-- id: insert_multirow
-- title: Insert multiple rows in one statement
-- owner: E1
CREATE TABLE r0_insert_multirow (id integer PRIMARY KEY, name text);
INSERT INTO r0_insert_multirow (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT id, name FROM r0_insert_multirow ORDER BY id;
