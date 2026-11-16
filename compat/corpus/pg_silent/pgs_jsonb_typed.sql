-- case: pgs_jsonb_typed
-- title: jsonb concatenation with typed
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / || between jsonb column and string literal
CREATE TABLE r0c_pgs_jsonb_typed (id int PRIMARY KEY, v jsonb);
INSERT INTO r0c_pgs_jsonb_typed VALUES (1, '{"a":1}');
SELECT v || '{"b":2}'::jsonb AS merged, pg_typeof(v || '{"b":2}'::jsonb) AS result_type FROM r0c_pgs_jsonb_typed;
