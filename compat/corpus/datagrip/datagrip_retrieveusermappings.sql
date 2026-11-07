-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveusermappings
-- title: latest-expanded-queries.json#dg_Batch_RetrieveUserMappings sha256:5b3890fbf57a
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveUserMappings
-- source-sha256: 5b3890fbf57a18342911cebae67bc118f263150293407ab19588bc8349749f14
CREATE TABLE r0c_datagrip_retrieveusermappings_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveusermappings_body ON r0c_datagrip_retrieveusermappings_app (body);
select umid as id,
       srvid as server_id,
       usename as user,
       umoptions as options
from pg_catalog.pg_user_mappings
order by server_id
;
