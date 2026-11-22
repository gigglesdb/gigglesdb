-- case: pgf_uuid
-- title: UUID generation properties and fixed extraction
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-uuid.html
CREATE TABLE r0c_pgf_uuid (id integer PRIMARY KEY, u uuid);
INSERT INTO r0c_pgf_uuid VALUES (1,'019535d9-3df7-79fb-b466-fa907fa17f9e'),(2,'550e8400-e29b-41d4-a716-446655440000'),(3,NULL);
SELECT id, uuid_extract_version(u) FROM r0c_pgf_uuid ORDER BY id;
SELECT id, uuid_extract_timestamp(u) FROM r0c_pgf_uuid ORDER BY id;
SELECT id, u::text FROM r0c_pgf_uuid ORDER BY id;
SELECT uuidv4()::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
SELECT uuidv7()::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
SELECT uuidv7() IS NOT NULL;
SELECT gen_random_uuid()::text LIKE '________-____-____-____-____________';
SELECT uuidv7(interval '-1 day')::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
SELECT uuid_extract_timestamp(NULL::uuid);
DROP TABLE r0c_pgf_uuid;
