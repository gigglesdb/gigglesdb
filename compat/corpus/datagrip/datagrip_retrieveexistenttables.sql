-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveexistenttables
-- title: app-expanded-queries.json#dg_App_RetrieveExistentTables sha256:4701f50827f5
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/app-expanded-queries.json#dg_App_RetrieveExistentTables
-- source-sha256: 4701f50827f5f6f2d54eed818aedf17d0fc831d1ea46cf2328ee81a2a01b8417
-- parameters: :schema_id=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrieveexistenttables_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveexistenttables_body ON r0c_datagrip_retrieveexistenttables_app (body);
select oid from pg_catalog.pg_class C
where relkind in ('r', 'm', 'v', 'p', 'f')
  and relnamespace = 2200::oid
;
