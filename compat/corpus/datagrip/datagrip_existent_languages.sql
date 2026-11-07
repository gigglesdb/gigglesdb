-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_existent_languages
-- title: datagrip_existent_languages.sql#datagrip_existent_languages sha256:04c22678c132
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_existent_languages.sql#datagrip_existent_languages
-- source-sha256: 04c22678c132650e496d65b79016f9612ab974639ab9df38b3ce2fdb44564319
CREATE TABLE r0c_datagrip_existent_languages_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_existent_languages_body ON r0c_datagrip_existent_languages_app (body);
-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select oid from pg_catalog.pg_language;
