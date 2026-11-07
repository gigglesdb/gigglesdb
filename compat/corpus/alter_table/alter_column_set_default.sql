-- case: alter_column_set_default
-- title: Alter column set default
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_alter_column_set_default_t (id int PRIMARY KEY, score int);
INSERT INTO r0c_alter_column_set_default_t (id) VALUES (1);
ALTER TABLE r0c_alter_column_set_default_t ALTER COLUMN score SET DEFAULT 8;
INSERT INTO r0c_alter_column_set_default_t (id) VALUES (2);
SELECT id, score FROM r0c_alter_column_set_default_t ORDER BY id;
