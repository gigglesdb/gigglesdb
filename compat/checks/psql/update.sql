-- id: update
-- title: Update a selected row
-- owner: E1
CREATE TABLE r0_update (id integer PRIMARY KEY, name text);
INSERT INTO r0_update (id, name) VALUES (1, 'a');
INSERT INTO r0_update (id, name) VALUES (2, 'b');
UPDATE r0_update SET name = 'z' WHERE id = 1;
SELECT id, name FROM r0_update ORDER BY id;
