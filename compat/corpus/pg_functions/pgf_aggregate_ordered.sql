-- case: pgf_aggregate_ordered
-- title: Ordered set percentile mode and hypothetical rank
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-aggregate.html
CREATE TABLE r0c_pgf_aggregate_ordered (id integer PRIMARY KEY, x integer, y integer, s text, b boolean);
INSERT INTO r0c_pgf_aggregate_ordered VALUES (1,1,2,'b',true),(2,2,5,'a',false),(3,2,7,'a',NULL),(4,NULL,NULL,NULL,NULL);
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT percentile_cont(ARRAY[0.25,0.5,0.75]) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT mode() WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT rank(2) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT dense_rank(2) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT percent_rank(2) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT cume_dist(2) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered WHERE id>9;
SELECT mode() WITHIN GROUP (ORDER BY x) FROM r0c_pgf_aggregate_ordered WHERE id>9;
DROP TABLE r0c_pgf_aggregate_ordered;
