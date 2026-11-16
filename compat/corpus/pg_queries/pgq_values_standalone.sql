-- case: pgq_values_standalone
-- title: values standalone
-- owner: E3
-- expect: compatible
-- source: Differences: values standalone; PostgreSQL 18 SELECT
VALUES (2,upper('b')),(1,'a'||'!') ORDER BY 1;
