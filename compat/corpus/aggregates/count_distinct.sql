-- case: count_distinct
-- title: Distinct counts ignore null and numeric scale
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_count_distinct_t(id int PRIMARY KEY,k int,v int,n numeric,t text);
INSERT INTO r0c_count_distinct_t VALUES(1,1,1,1.0,'a'),(2,1,1,1.00,'a'),(3,1,2,2,'b'),(4,2,NULL,NULL,NULL),(5,2,2,2,'b');
SELECT count(*),count(v),count(DISTINCT v),count(DISTINCT n),count(DISTINCT t) FROM r0c_count_distinct_t;
SELECT k,count(DISTINCT v),count(DISTINCT n),count(DISTINCT t) FROM r0c_count_distinct_t GROUP BY k ORDER BY k;