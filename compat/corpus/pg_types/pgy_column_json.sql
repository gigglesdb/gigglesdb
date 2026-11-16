-- case: pgy_column_json
-- title: column json
-- owner: E2
-- expect: compatible
-- source: Differences / Data types: type names, unsupported types, ranges, geometry and text search; types.txt
CREATE TABLE r0c_pgy_column_json (id int PRIMARY KEY, v json);
INSERT INTO r0c_pgy_column_json VALUES (1, '{"a":1}'), (2, '{"a":1}');
SELECT id, v, v::text = v::text, v->>'a' FROM r0c_pgy_column_json ORDER BY v::text, id;
