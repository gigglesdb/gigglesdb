-- id: create_table
-- title: Create a table with supported column types
-- owner: E1
CREATE TABLE r0_create_table (id integer PRIMARY KEY, name text NOT NULL, score bigint, active boolean);
SELECT count(*) FROM r0_create_table;
