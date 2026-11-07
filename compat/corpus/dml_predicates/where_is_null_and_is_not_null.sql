-- case: where_is_null_and_is_not_null
-- title: Where is null and is not null
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_where_is_null_and_is_not_null_t (id int PRIMARY KEY, name text);
INSERT INTO r0c_where_is_null_and_is_not_null_t VALUES (1, NULL);
INSERT INTO r0c_where_is_null_and_is_not_null_t VALUES (2, 'present');
SELECT id FROM r0c_where_is_null_and_is_not_null_t WHERE name IS NULL ORDER BY id;
SELECT id FROM r0c_where_is_null_and_is_not_null_t WHERE name IS NOT NULL ORDER BY id;
