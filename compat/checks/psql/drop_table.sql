-- id: drop_table
-- title: Drop a table and verify catalog removal
-- owner: E1
CREATE TABLE r0_drop_table (id integer PRIMARY KEY, name text);
INSERT INTO r0_drop_table (id, name) VALUES (1, 'a');
DROP TABLE r0_drop_table;
SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'r0_drop_table';
