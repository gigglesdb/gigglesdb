-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveforeigndatawrappers
-- title: latest-expanded-queries.json#dg_Batch_RetrieveForeignDataWrappers sha256:a87f59249c2d
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveForeignDataWrappers
-- source-sha256: a87f59249c2d1e16b08e7a20f7b36d0ed23cb883f5ce27bda98de077ca27a871
CREATE TABLE r0c_datagrip_retrieveforeigndatawrappers_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveforeigndatawrappers_body ON r0c_datagrip_retrieveforeigndatawrappers_app (body);
select fdw.oid as id,
       fdw.xmin as state_number,
       fdw.fdwname as name,
       pr.proname as handler,
       nspc.nspname as handler_schema,
       pr2.proname as validator,
       nspc2.nspname as validator_schema,
       fdw.fdwoptions as options,
       pg_catalog.pg_get_userbyid(fdw.fdwowner) as "owner"
from pg_catalog.pg_foreign_data_wrapper fdw
     left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid
     left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid
     left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid
     left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid
--  where pg_catalog.age(fdw.xmin) <= #TXAGE
;
