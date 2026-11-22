-- case: pgf_uuid_gen_random_v7
-- title: gen_random_uuid() version (GigglesDB returns UUIDv7, PostgreSQL 18.6 returns v4)
-- owner: DD-0068
-- expect: intentional gen_random_uuid() returns UUIDv7 because v7 is better for indexes (time-ordered keys keep B-tree inserts local instead of scattering them), so we prefer it; uuidv4() gives PostgreSQL's v4 (user decision 2026-11-22)
-- source: Differences Functions and operators; https://www.postgresql.org/docs/18/functions-uuid.html
SELECT substr(gen_random_uuid()::text, 15, 1) = '4';
SELECT uuid_extract_version(gen_random_uuid());
SELECT uuid_extract_timestamp(gen_random_uuid()) IS NULL;
