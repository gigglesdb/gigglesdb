-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_full_schema_object_acls
-- title: datagrip_full_schema_object_acls.sql#datagrip_full_schema_object_acls sha256:13ed14e3b27d
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_full_schema_object_acls.sql#datagrip_full_schema_object_acls
-- source-sha256: 13ed14e3b27d6edaee9824585e99c8c4b84d11339e1efca1819c4efda1700714
-- parameters: $1=2200, $2=2200, $3=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_full_schema_object_acls_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_full_schema_object_acls_body ON r0c_datagrip_full_schema_object_acls_app (body);
select T.oid as object_id,
                 T.relacl as acl
          from pg_catalog.pg_class T
          where relnamespace = 2200::oid 
          union all
          select T.oid as object_id,
                 T.proacl as acl
          from pg_catalog.pg_proc T
          where pronamespace = 2200::oid 
          union all
          select T.oid as object_id,
                 T.typacl as acl
          from pg_catalog.pg_type T
          where typnamespace = 2200::oid 
          order by object_id
;
