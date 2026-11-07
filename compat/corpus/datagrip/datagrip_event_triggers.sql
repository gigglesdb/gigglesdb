-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_event_triggers
-- title: datagrip_event_triggers.sql#datagrip_event_triggers sha256:2a12f8115db1
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_event_triggers.sql#datagrip_event_triggers
-- source-sha256: 2a12f8115db18ff54de46fec4f1a2473a5b8d5e64c9d2a799ab0ac9da9a38a60
CREATE TABLE r0c_datagrip_event_triggers_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_event_triggers_body ON r0c_datagrip_event_triggers_app (body);
-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select t.oid as id,
       t.xmin as state_number,
       t.evtname as name,
       t.evtevent as event,
       t.evtfoid as routine_id,
       pg_catalog.pg_get_userbyid(t.evtowner) as owner,
       t.evttags as tags,
       case when t.evtenabled = 'D' then 1 else 0 end as is_disabled
from pg_catalog.pg_event_trigger t;
