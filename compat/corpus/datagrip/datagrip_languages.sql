-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_languages
-- title: datagrip_languages.sql#datagrip_languages sha256:a3dec296b733
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_languages.sql#datagrip_languages
-- source-sha256: a3dec296b73336524c7c0e39ae386a9438e6b26df743d84ad99f85f8fd454e99
CREATE TABLE r0c_datagrip_languages_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_languages_body ON r0c_datagrip_languages_app (body);
-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted,
       h.proname as handler, hs.nspname as handlerSchema,
       i.proname as inline, isc.nspname as inlineSchema,
       v.proname as validator, vs.nspname as validatorSchema
from pg_catalog.pg_language l
    left join pg_catalog.pg_proc h on h.oid = lanplcallfoid
    left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace
    left join pg_catalog.pg_proc i on i.oid = laninline
    left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace
    left join pg_catalog.pg_proc v on v.oid = lanvalidator
    left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace
order by lanname;
