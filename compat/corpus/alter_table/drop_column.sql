-- case: drop_column
-- title: Drop column
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_drop_column_t (id int PRIMARY KEY, obsolete text);
INSERT INTO r0c_drop_column_t VALUES (1, 'old');
ALTER TABLE r0c_drop_column_t DROP COLUMN obsolete;
SELECT * FROM r0c_drop_column_t ORDER BY id;
