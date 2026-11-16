-- case: pgf_set_subscripts
-- title: Subscripts ordinality and select list SRFs
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-srf.html
CREATE TABLE r0c_pgf_set_subscripts (id integer PRIMARY KEY, a integer[]);
INSERT INTO r0c_pgf_set_subscripts VALUES (1,ARRAY[3,2,1]),(2,ARRAY[]::integer[]),(3,NULL);
SELECT id,n,a[n] FROM r0c_pgf_set_subscripts CROSS JOIN LATERAL generate_subscripts(a,1) AS g(n) ORDER BY id,n;
SELECT id,generate_subscripts(a,1) AS n FROM r0c_pgf_set_subscripts ORDER BY id,n;
SELECT id,unnest(a) AS v FROM r0c_pgf_set_subscripts ORDER BY id,v;
SELECT n FROM generate_subscripts(ARRAY[[1,2],[3,4]],2,true) AS g(n) ORDER BY n;
SELECT v,n FROM unnest(ARRAY['b',NULL,'a']) WITH ORDINALITY AS u(v,n) ORDER BY n;
SELECT generate_series(1,2) AS n ORDER BY n NULLS LAST;
SELECT unnest(ARRAY['a','b','c']) AS v ORDER BY v;
DROP TABLE r0c_pgf_set_subscripts;
