-- case: pgq_cte_recursive_series
-- title: cte recursive series
-- owner: E3
-- expect: compatible
-- source: Differences: WITH and WITH RECURSIVE; PostgreSQL 18 WITH
WITH RECURSIVE days(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM days WHERE n<5) SELECT n FROM days ORDER BY n;
