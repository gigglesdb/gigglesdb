-- case: pgq_window_grouping_sets
-- title: Window functions over grouping sets
-- owner: E3
-- expect: compatible
-- source: GD-0092; PostgreSQL 18 SELECT
CREATE TABLE r0c_pgq_window_grouping_sets(a int, b text, v int);
INSERT INTO r0c_pgq_window_grouping_sets
VALUES (1,'x',10),(1,'y',20),(3,'x',30),(3,NULL,40),(NULL,'y',50);

SELECT a, b, sum(v) AS s, grouping(a, b) AS g,
       rank() OVER (PARTITION BY grouping(a, b) ORDER BY sum(v) DESC)
FROM r0c_pgq_window_grouping_sets
GROUP BY ROLLUP(a, b)
ORDER BY g, a, b;

SELECT a, b, sum(v) AS s, sum(sum(v)) OVER (ORDER BY grouping(a, b), a, b)
FROM r0c_pgq_window_grouping_sets
GROUP BY CUBE(a, b)
ORDER BY grouping(a, b), a, b;

SELECT a, b, count(*) AS c,
       lag(count(*)) OVER (ORDER BY a NULLS FIRST, b NULLS FIRST, grouping(a, b))
FROM r0c_pgq_window_grouping_sets
GROUP BY GROUPING SETS ((a), (b), ())
ORDER BY a NULLS FIRST, b NULLS FIRST, grouping(a, b);

SELECT a, grouping(a), count(*), row_number() OVER (ORDER BY grouping(a) DESC, a)
FROM r0c_pgq_window_grouping_sets
GROUP BY ROLLUP(a)
ORDER BY grouping(a) DESC, a;

SELECT a, row_number() OVER (ORDER BY a DESC NULLS LAST)
FROM r0c_pgq_window_grouping_sets
GROUP BY ROLLUP(a)
ORDER BY 2 LIMIT 2;

SELECT a, b, count(*) OVER ()
FROM r0c_pgq_window_grouping_sets
GROUP BY DISTINCT GROUPING SETS ((a, b), (b, a), (r0c_pgq_window_grouping_sets.a, b))
ORDER BY a, b;
