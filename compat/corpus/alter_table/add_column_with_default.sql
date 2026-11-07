-- case: add_column_with_default
-- title: Add column with default
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_add_column_with_default_t (id int PRIMARY KEY);
INSERT INTO r0c_add_column_with_default_t VALUES (1);
ALTER TABLE r0c_add_column_with_default_t ADD COLUMN score int DEFAULT 7;
INSERT INTO r0c_add_column_with_default_t (id) VALUES (2);
SELECT id, score FROM r0c_add_column_with_default_t ORDER BY id;
