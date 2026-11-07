-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_catalog_acls
-- title: latest-expanded-queries.json#dg_Batch_SchemaForeignACL sha256:29995a6946a2
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_SchemaForeignACL
-- source-sha256: 29995a6946a247183c1f8390709217bed2ea9875b1c289c85e991a9096f54109
CREATE TABLE r0c_datagrip_catalog_acls_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_catalog_acls_body ON r0c_datagrip_catalog_acls_app (body);
select T.oid as object_id,
                 T.fdwacl as acl
          from pg_catalog.pg_foreign_data_wrapper T 
          union all
          select T.oid as object_id,
                 T.lanacl as acl
          from pg_catalog.pg_language T 
          union all
          select T.oid as object_id,
                 T.nspacl as acl
          from pg_catalog.pg_namespace T 
          union all
          select T.oid as object_id,
                 T.srvacl as acl
          from pg_catalog.pg_foreign_server T 
          
;
