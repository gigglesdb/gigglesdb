-- case: unique_multi_column
-- title: Unique multi column
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_unique_multi_column_t (id int PRIMARY KEY, a int, b int, CONSTRAINT r0c_unique_multi_column_uq UNIQUE (a, b));
INSERT INTO r0c_unique_multi_column_t VALUES (1, 5, 6);
INSERT INTO r0c_unique_multi_column_t VALUES (2, 5, 7);
-- @error
-- expect-sqlstate: 23505
INSERT INTO r0c_unique_multi_column_t VALUES (3, 5, 6);
SELECT id, a, b FROM r0c_unique_multi_column_t ORDER BY id;
