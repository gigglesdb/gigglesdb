-- case: sum_avg_distinct_grouped
-- title: Grouped distinct aggregates in projection HAVING and ORDER
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_sum_avg_distinct_grouped_t(id int PRIMARY KEY,k int,v int,n numeric);
INSERT INTO r0c_sum_avg_distinct_grouped_t VALUES(1,1,1,1.0),(2,1,1,1.00),(3,1,2,2),(4,2,4,4),(5,2,NULL,NULL);
SELECT k,sum(v),sum(DISTINCT v),avg(v),avg(DISTINCT v),min(DISTINCT v),max(DISTINCT v),avg(DISTINCT n) FROM r0c_sum_avg_distinct_grouped_t GROUP BY k ORDER BY k;
SELECT k,avg(DISTINCT v)+1 FROM r0c_sum_avg_distinct_grouped_t GROUP BY k HAVING count(DISTINCT v)>0 ORDER BY avg(DISTINCT v) DESC,k;
SELECT count(DISTINCT v),sum(DISTINCT v),avg(DISTINCT v) FROM r0c_sum_avg_distinct_grouped_t WHERE id<0;