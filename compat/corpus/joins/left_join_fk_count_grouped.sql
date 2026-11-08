-- case: left_join_fk_count_grouped
-- title: Grouped count over nullable join side
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_left_join_fk_count_grouped_t (id int PRIMARY KEY, k int);
CREATE TABLE r0c_left_join_fk_count_grouped_u (id int PRIMARY KEY, k int);
INSERT INTO r0c_left_join_fk_count_grouped_t VALUES (1,10),(2,20),(3,NULL);
INSERT INTO r0c_left_join_fk_count_grouped_u VALUES (11,10),(12,10),(13,30);
SELECT t.id, count(*), count(u.id) FROM r0c_left_join_fk_count_grouped_t t LEFT JOIN r0c_left_join_fk_count_grouped_u u ON t.k = u.k GROUP BY t.id ORDER BY t.id;
