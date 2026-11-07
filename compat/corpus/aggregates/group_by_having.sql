-- case: group_by_having
-- title: Grouped aggregate HAVING predicates
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_group_by_having_t (id int PRIMARY KEY, k int, v int);
INSERT INTO r0c_group_by_having_t VALUES (1,1,10),(2,1,20),(3,2,5),(4,3,40),(5,3,50);
SELECT k, count(*), sum(v) FROM r0c_group_by_having_t GROUP BY k HAVING count(*) > 1 ORDER BY k;
SELECT k, sum(v) FROM r0c_group_by_having_t GROUP BY k HAVING sum(v) > 20 ORDER BY k;
