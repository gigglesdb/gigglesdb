-- case: rename_column
-- title: Rename column
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_rename_column_t (id int PRIMARY KEY, old_name text);
INSERT INTO r0c_rename_column_t VALUES (1, 'value');
ALTER TABLE r0c_rename_column_t RENAME COLUMN old_name TO new_name;
SELECT id, new_name FROM r0c_rename_column_t ORDER BY id;
