-- id: select
-- title: Select filtered rows and aggregates
-- owner: E3
CREATE TABLE r0_select (id integer PRIMARY KEY, name text);
INSERT INTO r0_select (id, name) VALUES (1, 'a');
INSERT INTO r0_select (id, name) VALUES (2, 'b');
INSERT INTO r0_select (id, name) VALUES (3, 'c');
SELECT id, name FROM r0_select WHERE id >= 2 ORDER BY id;
SELECT count(*) FROM r0_select;
SELECT name FROM r0_select WHERE id = 1;
