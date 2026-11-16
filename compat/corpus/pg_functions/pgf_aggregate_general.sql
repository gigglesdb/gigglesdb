-- case: pgf_aggregate_general
-- title: Ordered boolean bitwise and filtered aggregates
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-aggregate.html
CREATE TABLE r0c_pgf_aggregate_general (id integer PRIMARY KEY, x integer, y integer, s text, b boolean);
INSERT INTO r0c_pgf_aggregate_general VALUES (1,1,2,'b',true),(2,2,5,'a',false),(3,2,7,'a',NULL),(4,NULL,NULL,NULL,NULL);
SELECT string_agg(s,',' ORDER BY id) FROM r0c_pgf_aggregate_general;
SELECT array_agg(x ORDER BY id) FROM r0c_pgf_aggregate_general;
SELECT bool_and(b) FROM r0c_pgf_aggregate_general;
SELECT bool_or(b) FROM r0c_pgf_aggregate_general;
SELECT every(b) FROM r0c_pgf_aggregate_general;
SELECT bit_and(x) FROM r0c_pgf_aggregate_general;
SELECT bit_or(x) FROM r0c_pgf_aggregate_general;
SELECT bit_xor(x) FROM r0c_pgf_aggregate_general;
SELECT any_value(x) IN (1,2) FROM r0c_pgf_aggregate_general;
SELECT sum(x) FILTER (WHERE b) FROM r0c_pgf_aggregate_general;
SELECT count(DISTINCT x) FROM r0c_pgf_aggregate_general;
SELECT string_agg(DISTINCT s,',' ORDER BY s) FROM r0c_pgf_aggregate_general;
SELECT bool_and(b) FROM r0c_pgf_aggregate_general WHERE id>9;
SELECT bit_and(x) FROM r0c_pgf_aggregate_general WHERE id>9;
SELECT array_agg(x ORDER BY id) FROM r0c_pgf_aggregate_general WHERE id>9;
SELECT string_agg(s,',' ORDER BY id) FROM r0c_pgf_aggregate_general WHERE id>9;
SELECT bool_and(true);
SELECT bool_or(false);
SELECT bit_xor(3);
DROP TABLE r0c_pgf_aggregate_general;
