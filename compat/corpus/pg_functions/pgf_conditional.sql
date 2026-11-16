-- case: pgf_conditional
-- title: Conditional expressions and NULL propagation
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-conditional.html
CREATE TABLE r0c_pgf_conditional (id integer PRIMARY KEY, x integer, s text);
INSERT INTO r0c_pgf_conditional VALUES (1,2,'paid'),(2,0,''),(3,NULL,NULL);
SELECT id, nullif(x,0) FROM r0c_pgf_conditional ORDER BY id;
SELECT id, coalesce(x,99) FROM r0c_pgf_conditional ORDER BY id;
SELECT id, greatest(x,1,NULL) FROM r0c_pgf_conditional ORDER BY id;
SELECT id, least(x,1,NULL) FROM r0c_pgf_conditional ORDER BY id;
SELECT id, CASE WHEN x>0 THEN s ELSE 'missing' END FROM r0c_pgf_conditional ORDER BY id;
SELECT id, CASE x WHEN 2 THEN 'two' WHEN 0 THEN 'zero' ELSE 'other' END FROM r0c_pgf_conditional ORDER BY id;
SELECT id, nullif(3,3) FROM r0c_pgf_conditional ORDER BY id;
SELECT id, coalesce(NULL::text,'fallback') FROM r0c_pgf_conditional ORDER BY id;
SELECT id, greatest(NULL::integer,NULL) FROM r0c_pgf_conditional ORDER BY id;
SELECT id, least(NULL::integer,NULL) FROM r0c_pgf_conditional ORDER BY id;
DROP TABLE r0c_pgf_conditional;
