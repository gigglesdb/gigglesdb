-- id: delete
-- title: Delete a selected row
-- owner: E1
CREATE TABLE r0_delete (id integer PRIMARY KEY, name text);
INSERT INTO r0_delete (id, name) VALUES (1, 'a');
INSERT INTO r0_delete (id, name) VALUES (2, 'b');
INSERT INTO r0_delete (id, name) VALUES (3, 'c');
DELETE FROM r0_delete WHERE id = 2;
SELECT id, name FROM r0_delete ORDER BY id;
