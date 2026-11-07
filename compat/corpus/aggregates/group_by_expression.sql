-- case: group_by_expression
-- title: Grouping and aggregate expressions
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_group_by_expression_t (id int PRIMARY KEY, k int, v int);
INSERT INTO r0c_group_by_expression_t VALUES (1,1,10),(2,1,20),(3,2,30),(4,3,40);
SELECT k + 1 AS bucket, count(*), sum(v) FROM r0c_group_by_expression_t GROUP BY k + 1 ORDER BY bucket;
SELECT k, sum(v) + count(*) AS total FROM r0c_group_by_expression_t GROUP BY k ORDER BY k;
