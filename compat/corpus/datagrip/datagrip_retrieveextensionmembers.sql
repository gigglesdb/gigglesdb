-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveextensionmembers
-- title: latest-expanded-queries.json#dg_Batch_RetrieveExtensionMembers sha256:0776efd8392d
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveExtensionMembers
-- source-sha256: 0776efd8392dd1c5c28bab74fbdd7dd5dc417d4a4fd9962e508b4fdb4d8b7a56
CREATE TABLE r0c_datagrip_retrieveextensionmembers_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveextensionmembers_body ON r0c_datagrip_retrieveextensionmembers_app (body);
select E.oid   as extension_id,
       D.objid as member_id
from pg_extension E
     join pg_depend D on E.oid = D.refobjid and
                         D.refclassid = 'pg_extension'::regclass::oid
where D.deptype = 'e'
order by extension_id
;
