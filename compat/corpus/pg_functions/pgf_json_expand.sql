-- case: pgf_json_expand
-- title: JSON arrays and objects expanded into rows
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-json.html
CREATE TABLE r0c_pgf_json_expand (id integer PRIMARY KEY, a jsonb, o jsonb);
INSERT INTO r0c_pgf_json_expand VALUES (1,'[1,"x",null]','{"b":2,"a":1}'),(2,'[]','{}'),(3,NULL,NULL);
SELECT id,v,n FROM r0c_pgf_json_expand CROSS JOIN LATERAL jsonb_array_elements(a) WITH ORDINALITY AS e(v,n) ORDER BY id,n;
SELECT id,v,n FROM r0c_pgf_json_expand CROSS JOIN LATERAL jsonb_array_elements_text(a) WITH ORDINALITY AS e(v,n) ORDER BY id,n;
SELECT id,k,v FROM r0c_pgf_json_expand CROSS JOIN LATERAL jsonb_each(o) AS e(k,v) ORDER BY id,k;
SELECT id,k,v FROM r0c_pgf_json_expand CROSS JOIN LATERAL jsonb_each_text(o) AS e(k,v) ORDER BY id,k;
SELECT id,k FROM r0c_pgf_json_expand CROSS JOIN LATERAL jsonb_object_keys(o) AS e(k) ORDER BY id,k;
DROP TABLE r0c_pgf_json_expand;
