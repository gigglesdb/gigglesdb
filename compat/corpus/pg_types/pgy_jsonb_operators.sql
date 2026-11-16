-- case: pgy_jsonb_operators
-- title: jsonb operators
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_jsonb_operators (id int PRIMARY KEY,v jsonb);
INSERT INTO r0c_pgy_jsonb_operators VALUES (1,'{"a":{"b":[1,2]},"c":"x"}');
SELECT v#>'{a,b}',v#>>'{a,b,1}',v @> '{"c":"x"}',v <@ v,v ? 'c',v ?| ARRAY['c','d'],v ?& ARRAY['a','c'],v-'c',v||'{"d":1}',v||'{"d":1}'::jsonb FROM r0c_pgy_jsonb_operators ORDER BY id;
SELECT '{"a":1}'::jsonb->>'a',jsonb_typeof('[1]'::jsonb),json_typeof('[]'::json);
-- @error
SELECT '{bad}'::jsonb;
