-- case: pgf_aggregate_regression
-- title: Regression aggregate family
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-aggregate.html
CREATE TABLE r0c_pgf_aggregate_regression (id integer PRIMARY KEY, x integer, y integer, s text, b boolean);
INSERT INTO r0c_pgf_aggregate_regression VALUES (1,1,2,'b',true),(2,2,5,'a',false),(3,2,7,'a',NULL),(4,NULL,NULL,NULL,NULL);
SELECT regr_count(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_avgx(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_avgy(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_slope(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_intercept(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_r2(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_sxx(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_syy(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_sxy(y,x) FROM r0c_pgf_aggregate_regression;
SELECT regr_count(y,x) FROM r0c_pgf_aggregate_regression WHERE id>9;
SELECT regr_slope(y,x) FROM r0c_pgf_aggregate_regression WHERE id>9;
SELECT regr_count(2,1);
SELECT regr_sxx(2,1);
DROP TABLE r0c_pgf_aggregate_regression;
