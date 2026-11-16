-- case: pgf_json_sql
-- title: SQL JSON value query exists and validation
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-json.html
CREATE TABLE r0c_pgf_json_sql (id integer PRIMARY KEY, j jsonb);
INSERT INTO r0c_pgf_json_sql VALUES (1,'{"a":[1,2],"s":"yes"}'),(2,'{}'),(3,NULL);
SELECT id, JSON_VALUE(j,'$.s') FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, JSON_VALUE(j,'$.a[0]' RETURNING integer) FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, JSON_QUERY(j,'$.a') FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, JSON_EXISTS(j,'$.a[*] ? (@ > 1)') FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, j::text IS JSON FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, j::text IS JSON OBJECT FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, '[1,2]' IS JSON ARRAY FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, 'true' IS JSON SCALAR FROM r0c_pgf_json_sql ORDER BY id;
SELECT id, '{"a":1,"a":2}' IS JSON WITH UNIQUE KEYS FROM r0c_pgf_json_sql ORDER BY id;
DROP TABLE r0c_pgf_json_sql;
