-- case: pgy_composite_nested
-- title: composite nested
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: composite attributes of user-defined types; types.txt / Composite types
CREATE DOMAIN r0c_pgy_composite_nested_d AS int CHECK (VALUE > 0);
CREATE TYPE r0c_pgy_composite_nested_pair AS (a int,b text);
CREATE TYPE r0c_pgy_composite_nested_outer AS (pair r0c_pgy_composite_nested_pair, items int[], checked r0c_pgy_composite_nested_d);
CREATE TABLE r0c_pgy_composite_nested (id int PRIMARY KEY,v r0c_pgy_composite_nested_outer);
INSERT INTO r0c_pgy_composite_nested VALUES (1,ROW(ROW(2,'x')::r0c_pgy_composite_nested_pair,ARRAY[3,4],5)::r0c_pgy_composite_nested_outer);
SELECT (v).pair, ((v).pair).b, (v).items[2], (v).checked, v = v FROM r0c_pgy_composite_nested ORDER BY v,id;
UPDATE r0c_pgy_composite_nested SET v.pair.a = 8 WHERE id=1;
SELECT ((v).pair).a FROM r0c_pgy_composite_nested ORDER BY id;
SELECT ROW(1,'a')::r0c_pgy_composite_nested_pair < ROW(2,'b')::r0c_pgy_composite_nested_pair;
DROP TABLE r0c_pgy_composite_nested;
DROP TYPE r0c_pgy_composite_nested_outer;
DROP TYPE r0c_pgy_composite_nested_pair;
DROP DOMAIN r0c_pgy_composite_nested_d;
