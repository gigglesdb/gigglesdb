-- case: pgy_uuid_input
-- title: uuid input
-- owner: E2
-- expect: compatible
-- source: Data types; types.txt
CREATE TABLE r0c_pgy_uuid_input (id uuid PRIMARY KEY, v uuid UNIQUE);
INSERT INTO r0c_pgy_uuid_input VALUES ('{123E4567-E89B-12D3-A456-426614174000}','123e4567e89b12d3a456426614174001');
CREATE INDEX r0c_pgy_uuid_input_idx ON r0c_pgy_uuid_input(v);
SELECT id,v,id<v FROM r0c_pgy_uuid_input ORDER BY id;
SELECT length(gen_random_uuid()::text)=36;
-- @error
SELECT 'not-a-uuid'::uuid;
-- @error
SELECT min(id) FROM r0c_pgy_uuid_input;
-- @error
SELECT max(id) FROM r0c_pgy_uuid_input;
