-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveextensions
-- title: latest-expanded-queries.json#dg_Batch_RetrieveExtensions sha256:1cc79daf5adb
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveExtensions
-- source-sha256: 1cc79daf5adbec3ec155609c45ac43c50a803aae9845bb42f375556d38ad2c63
CREATE TABLE r0c_datagrip_retrieveextensions_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveextensions_body ON r0c_datagrip_retrieveextensions_app (body);
select E.oid        as id,
       E.xmin       as state_number,
       extname      as name,
       extversion   as version,
       extnamespace as schema_id,
       nspname      as schema_name
       ,
       array(select unnest
             from unnest(available_versions)
             where unnest > extversion) as available_updates
       
from pg_catalog.pg_extension E
       join pg_namespace N on E.extnamespace = N.oid
       left join (select name, array_agg(version) as available_versions
                  from pg_available_extension_versions()
                  group by name) V on E.extname = V.name
       
--  where pg_catalog.age(E.xmin) <= #TXAGE
;
