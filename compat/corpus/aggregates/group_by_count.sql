-- case: group_by_count
-- title: Grouped counts and nullable values
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_group_by_count_t (id int PRIMARY KEY, k int, v int);
INSERT INTO r0c_group_by_count_t VALUES (1,1,10),(2,1,NULL),(3,2,30),(4,NULL,40),(5,NULL,NULL);
SELECT k, count(*), count(v), sum(v), min(v), max(v) FROM r0c_group_by_count_t GROUP BY k ORDER BY k;
