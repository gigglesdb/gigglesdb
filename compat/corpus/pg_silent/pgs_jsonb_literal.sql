-- case: pgs_jsonb_literal
-- title: jsonb concatenation with literal
-- owner: DD-0068
-- expect: compatible
-- source: Differences: Read first / || between jsonb column and string literal
CREATE TABLE r0c_pgs_jsonb_literal (id int PRIMARY KEY, v jsonb);
INSERT INTO r0c_pgs_jsonb_literal VALUES (1, '{"a":1}');
SELECT id, v || '{"b":2}' FROM r0c_pgs_jsonb_literal ORDER BY id;
SELECT id, pg_typeof(v || '{"b":2}') AS result_type FROM r0c_pgs_jsonb_literal ORDER BY id;
