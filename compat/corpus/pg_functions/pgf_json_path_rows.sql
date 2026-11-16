-- case: pgf_json_path_rows
-- title: JSON path query set returning output
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-json.html
CREATE TABLE r0c_pgf_json_path_rows (id integer PRIMARY KEY, j jsonb);
INSERT INTO r0c_pgf_json_path_rows VALUES (1,'{"a":[1,2,3]}'),(2,'{"a":[]}'),(3,NULL);
SELECT id,v,n FROM r0c_pgf_json_path_rows CROSS JOIN LATERAL jsonb_path_query(j,'$.a[*]') WITH ORDINALITY AS q(v,n) ORDER BY id,n;
SELECT v,n FROM jsonb_path_query('[1,2,3]','$[*] ? (@ > 1)') WITH ORDINALITY AS q(v,n) ORDER BY n;
DROP TABLE r0c_pgf_json_path_rows;
