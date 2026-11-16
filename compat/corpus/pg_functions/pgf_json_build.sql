-- case: pgf_json_build
-- title: JSON construction modification and presentation
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-json.html
CREATE TABLE r0c_pgf_json_build (id integer PRIMARY KEY, j jsonb);
INSERT INTO r0c_pgf_json_build VALUES (1,'{"a":[1,2],"b":null}'),(2,'{}'),(3,NULL);
SELECT id, jsonb_build_object('id',id,'value',j) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, jsonb_build_array(id,j,NULL) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, to_jsonb(id) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, row_to_json(ROW(id,j)) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, jsonb_set(j,'{a,0}','9',false) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, jsonb_insert(j,'{a,0}','8') FROM r0c_pgf_json_build ORDER BY id;
SELECT id, jsonb_strip_nulls(j) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, jsonb_pretty(j) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, jsonb_typeof(j) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, jsonb_array_length(CASE id WHEN 1 THEN '[1,2]'::jsonb ELSE NULL::jsonb END) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, json_build_object('a',1) FROM r0c_pgf_json_build ORDER BY id;
SELECT id, json_build_array(1,NULL,'x') FROM r0c_pgf_json_build ORDER BY id;
DROP TABLE r0c_pgf_json_build;
