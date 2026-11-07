-- case: alter_column_drop_not_null_and_set_not_null
-- title: Alter column drop not null and set not null
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_alter_column_drop_not_null_and_set_not_null_t (id int PRIMARY KEY, score int NOT NULL);
ALTER TABLE r0c_alter_column_drop_not_null_and_set_not_null_t ALTER COLUMN score DROP NOT NULL;
INSERT INTO r0c_alter_column_drop_not_null_and_set_not_null_t VALUES (1, NULL);
SELECT id, score FROM r0c_alter_column_drop_not_null_and_set_not_null_t ORDER BY id;
DELETE FROM r0c_alter_column_drop_not_null_and_set_not_null_t WHERE score IS NULL;
ALTER TABLE r0c_alter_column_drop_not_null_and_set_not_null_t ALTER COLUMN score SET NOT NULL;
-- @error
-- expect-sqlstate: 23502
INSERT INTO r0c_alter_column_drop_not_null_and_set_not_null_t VALUES (2, NULL);
INSERT INTO r0c_alter_column_drop_not_null_and_set_not_null_t VALUES (3, 9);
SELECT id, score FROM r0c_alter_column_drop_not_null_and_set_not_null_t ORDER BY id;
