-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_shared_acls
-- title: latest-expanded-queries.json#dg_Batch_DatabaseTablespaceACL sha256:1da2fa93dc01
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_DatabaseTablespaceACL
-- source-sha256: 1da2fa93dc01a18907929a1c0b30133fcc02af9d9da7864fffd6fb05323f2075
CREATE TABLE r0c_datagrip_shared_acls_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_shared_acls_body ON r0c_datagrip_shared_acls_app (body);
select T.oid as object_id,
                 T.spcacl as acl
          from pg_catalog.pg_tablespace T 
          union all
          select T.oid as object_id,
                 T.datacl as acl
          from pg_catalog.pg_database T 
;
