-- case: update_multiple_columns
-- title: Update multiple columns
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_update_multiple_columns_t (id int PRIMARY KEY, score int, name text);
INSERT INTO r0c_update_multiple_columns_t VALUES (1, 10, 'before');
UPDATE r0c_update_multiple_columns_t SET score = 20, name = 'after' WHERE id = 1;
SELECT id, score, name FROM r0c_update_multiple_columns_t ORDER BY id;
