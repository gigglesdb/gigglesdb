-- case: uuid_column
-- title: Uuid column
-- owner: E2
-- expect: compatible
CREATE TABLE r0c_uuid_column_t (id uuid);
INSERT INTO r0c_uuid_column_t VALUES ('123e4567-e89b-12d3-a456-426614174000');
SELECT id FROM r0c_uuid_column_t;
SELECT id FROM r0c_uuid_column_t WHERE id = '123e4567-e89b-12d3-a456-426614174000'::uuid;
