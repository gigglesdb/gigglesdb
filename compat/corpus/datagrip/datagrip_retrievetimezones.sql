-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrievetimezones
-- title: latest-expanded-queries.json#dg_Batch_RetrieveTimeZones sha256:44c936363ae3
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveTimeZones
-- source-sha256: 44c936363ae39ed03eec8fa839c78c755f5fa810884853149000db0fa6713374
CREATE TABLE r0c_datagrip_retrievetimezones_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrievetimezones_body ON r0c_datagrip_retrievetimezones_app (body);
select name, is_dst from pg_catalog.pg_timezone_names
union distinct
select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs
;
