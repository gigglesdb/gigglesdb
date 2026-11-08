-- case: left_join_basic
-- title: Left join preserves unmatched rows
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_left_join_basic_t (id int PRIMARY KEY, k int);
CREATE TABLE r0c_left_join_basic_u (id int PRIMARY KEY, k int);
INSERT INTO r0c_left_join_basic_t VALUES (1,10),(2,20),(3,NULL);
INSERT INTO r0c_left_join_basic_u VALUES (11,10),(12,10),(13,30);
SELECT t.id, u.id FROM r0c_left_join_basic_t t LEFT OUTER JOIN r0c_left_join_basic_u u ON t.k = u.k ORDER BY t.id, u.id;
