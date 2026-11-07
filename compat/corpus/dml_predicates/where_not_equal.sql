-- case: where_not_equal
-- title: Where not equal
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_where_not_equal_t (id int PRIMARY KEY);
INSERT INTO r0c_where_not_equal_t VALUES (1);
INSERT INTO r0c_where_not_equal_t VALUES (2);
SELECT id FROM r0c_where_not_equal_t WHERE id <> 1 ORDER BY id;
SELECT id FROM r0c_where_not_equal_t WHERE id != 2 ORDER BY id;
