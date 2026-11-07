-- case: jsonb_column
-- title: Jsonb column
-- owner: E2
-- expect: compatible
CREATE TABLE r0c_jsonb_column_t (data jsonb);
INSERT INTO r0c_jsonb_column_t VALUES ('{"a":1}');
SELECT data->>'a' FROM r0c_jsonb_column_t;
SELECT data FROM r0c_jsonb_column_t;
