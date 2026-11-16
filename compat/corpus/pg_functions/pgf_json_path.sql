-- case: pgf_json_path
-- title: JSON path operators and query functions
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-json.html
CREATE TABLE r0c_pgf_json_path (id integer PRIMARY KEY, j jsonb);
INSERT INTO r0c_pgf_json_path VALUES (1,'{"a":[1,2,3]}'),(2,'{"a":[]}'),(3,NULL);
SELECT id, j @? '$.a[*] ? (@ > 1)' FROM r0c_pgf_json_path ORDER BY id;
SELECT id, j @@ '$.a[0] == 1' FROM r0c_pgf_json_path ORDER BY id;
SELECT id, jsonb_path_exists(j,'$.a[*] ? (@ > 2)') FROM r0c_pgf_json_path ORDER BY id;
SELECT id, jsonb_path_match(j,'$.a[0] == 1') FROM r0c_pgf_json_path ORDER BY id;
SELECT id, jsonb_path_query_array(j,'$.a[*]') FROM r0c_pgf_json_path ORDER BY id;
SELECT id, jsonb_path_query_first(j,'$.a[*]') FROM r0c_pgf_json_path ORDER BY id;
SELECT id, jsonb_path_exists('{"x":3}', '$.x ? (@ > $min)', '{"min":2}') FROM r0c_pgf_json_path ORDER BY id;
DROP TABLE r0c_pgf_json_path;
