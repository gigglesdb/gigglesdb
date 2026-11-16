-- case: pgf_aggregate_statistics
-- title: Variance standard deviation covariance and correlation
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-aggregate.html
CREATE TABLE r0c_pgf_aggregate_statistics (id integer PRIMARY KEY, x integer, y integer, s text, b boolean);
INSERT INTO r0c_pgf_aggregate_statistics VALUES (1,1,2,'b',true),(2,2,5,'a',false),(3,2,7,'a',NULL),(4,NULL,NULL,NULL,NULL);
SELECT stddev(x) FROM r0c_pgf_aggregate_statistics;
SELECT stddev_samp(x) FROM r0c_pgf_aggregate_statistics;
SELECT stddev_pop(x) FROM r0c_pgf_aggregate_statistics;
SELECT variance(x) FROM r0c_pgf_aggregate_statistics;
SELECT var_samp(x) FROM r0c_pgf_aggregate_statistics;
SELECT var_pop(x) FROM r0c_pgf_aggregate_statistics;
SELECT covar_samp(y,x) FROM r0c_pgf_aggregate_statistics;
SELECT covar_pop(y,x) FROM r0c_pgf_aggregate_statistics;
SELECT corr(y,x) FROM r0c_pgf_aggregate_statistics;
SELECT stddev(x) FROM r0c_pgf_aggregate_statistics WHERE id>9;
SELECT var_pop(x) FROM r0c_pgf_aggregate_statistics WHERE id>9;
SELECT corr(y,x) FROM r0c_pgf_aggregate_statistics WHERE id>9;
SELECT stddev_pop(2);
SELECT var_pop(2);
DROP TABLE r0c_pgf_aggregate_statistics;
