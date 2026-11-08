-- case: left_join_on_vs_where
-- title: Left join ON filter versus WHERE filter
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_left_join_on_vs_where_t (id int PRIMARY KEY, k int);
CREATE TABLE r0c_left_join_on_vs_where_u (id int PRIMARY KEY, k int);
INSERT INTO r0c_left_join_on_vs_where_t VALUES (1,10),(2,20),(3,NULL);
INSERT INTO r0c_left_join_on_vs_where_u VALUES (11,10),(12,10),(13,30);
SELECT t.id, u.id FROM r0c_left_join_on_vs_where_t t LEFT JOIN r0c_left_join_on_vs_where_u u ON t.k = u.k AND u.id = 12 ORDER BY t.id, u.id;
SELECT t.id, u.id FROM r0c_left_join_on_vs_where_t t LEFT JOIN r0c_left_join_on_vs_where_u u ON t.k = u.k WHERE u.id = 12 ORDER BY t.id, u.id;
