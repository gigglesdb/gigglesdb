-- case: inner_join_non_equi
-- title: Inner join with inequality ON expression
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_inner_join_non_equi_t (id int PRIMARY KEY, k int);
CREATE TABLE r0c_inner_join_non_equi_u (id int PRIMARY KEY, k int);
INSERT INTO r0c_inner_join_non_equi_t VALUES (1,10),(2,20),(3,NULL);
INSERT INTO r0c_inner_join_non_equi_u VALUES (11,10),(12,10),(13,30);
SELECT t.id, u.id FROM r0c_inner_join_non_equi_t t INNER JOIN r0c_inner_join_non_equi_u u ON t.k < u.k ORDER BY t.id, u.id;
