-- case: rename_table
-- title: Rename table
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_rename_table_t (id int PRIMARY KEY);
INSERT INTO r0c_rename_table_t VALUES (1);
ALTER TABLE r0c_rename_table_t RENAME TO r0c_rename_table_u;
SELECT id FROM r0c_rename_table_u ORDER BY id;
