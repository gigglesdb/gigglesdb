-- case: pgf_comparison
-- title: Comparison spellings and symmetric bounds
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-comparison.html
CREATE TABLE r0c_pgf_comparison (id integer PRIMARY KEY, x integer);
INSERT INTO r0c_pgf_comparison VALUES (1,2),(2,0),(3,NULL);
SELECT id, x BETWEEN SYMMETRIC 3 AND 1 FROM r0c_pgf_comparison ORDER BY id;
SELECT id, x ISNULL FROM r0c_pgf_comparison ORDER BY id;
SELECT id, x NOTNULL FROM r0c_pgf_comparison ORDER BY id;
SELECT id, x IS DISTINCT FROM 2 FROM r0c_pgf_comparison ORDER BY id;
SELECT id, x IS NOT DISTINCT FROM NULL FROM r0c_pgf_comparison ORDER BY id;
SELECT id, num_nulls(x,NULL,1) FROM r0c_pgf_comparison ORDER BY id;
SELECT id, num_nonnulls(x,NULL,1) FROM r0c_pgf_comparison ORDER BY id;
SELECT id, 'abc' ~~ 'a%' FROM r0c_pgf_comparison ORDER BY id;
SELECT id, 'ABC' ~~* 'a%' FROM r0c_pgf_comparison ORDER BY id;
SELECT id, ROW(x,1) = ROW(2,1) FROM r0c_pgf_comparison ORDER BY id;
DROP TABLE r0c_pgf_comparison;
