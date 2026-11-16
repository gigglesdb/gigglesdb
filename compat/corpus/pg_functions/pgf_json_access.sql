-- case: pgf_json_access
-- title: JSON and JSONB extraction containment and existence
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-json.html
CREATE TABLE r0c_pgf_json_access (id integer PRIMARY KEY, j jsonb);
INSERT INTO r0c_pgf_json_access VALUES (1,'{"a":[1,2],"b":{"c":"x"},"null":null}'),(2,'{}'),(3,NULL);
SELECT id, j -> 'a' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j ->> 'b' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j #> '{a,0}' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j #>> '{b,c}' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j @> '{"a":[2]}'::jsonb FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j <@ '{}'::jsonb FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j ? 'a' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j ?| ARRAY['a','x'] FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j ?& ARRAY['a','b'] FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j || '{"d":3}' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j - 'a' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j #- '{b,c}' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, '{"a":1}'::json ->> 'a' FROM r0c_pgf_json_access ORDER BY id;
SELECT id, j::json::jsonb FROM r0c_pgf_json_access ORDER BY id;
DROP TABLE r0c_pgf_json_access;
