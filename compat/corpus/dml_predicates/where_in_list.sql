-- case: where_in_list
-- title: Where in list
-- owner: E1
-- expect: compatible
CREATE TABLE r0c_where_in_list_t (id int PRIMARY KEY);
INSERT INTO r0c_where_in_list_t VALUES (1);
INSERT INTO r0c_where_in_list_t VALUES (2);
INSERT INTO r0c_where_in_list_t VALUES (3);
SELECT id FROM r0c_where_in_list_t WHERE id IN (1, 3) ORDER BY id;
