-- case: inner_join_multi_condition
-- title: Inner join with multiple ON conditions
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_inner_join_multi_condition_t (id int PRIMARY KEY, k int);
CREATE TABLE r0c_inner_join_multi_condition_u (id int PRIMARY KEY, k int);
INSERT INTO r0c_inner_join_multi_condition_t VALUES (1,10),(2,20),(3,NULL);
INSERT INTO r0c_inner_join_multi_condition_u VALUES (11,10),(12,10),(13,30);
SELECT t.id, u.id FROM r0c_inner_join_multi_condition_t t INNER JOIN r0c_inner_join_multi_condition_u u ON t.k = u.k AND u.id > 11 ORDER BY t.id, u.id;
