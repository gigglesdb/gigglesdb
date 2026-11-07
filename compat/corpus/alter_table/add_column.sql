-- case: add_column
-- title: Add column
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_add_column_t (id int PRIMARY KEY);
INSERT INTO r0c_add_column_t VALUES (1);
ALTER TABLE r0c_add_column_t ADD COLUMN name text;
SELECT id, name FROM r0c_add_column_t ORDER BY id;
