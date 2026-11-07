-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveaccessmethods
-- title: latest-expanded-queries.json#dg_Batch_RetrieveAccessMethods sha256:adacca7f2fe6
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveAccessMethods
-- source-sha256: adacca7f2fe622ace6dfabd37a6fcd12a0f7c7f0d9937b055dc842e5fa116b42
CREATE TABLE r0c_datagrip_retrieveaccessmethods_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveaccessmethods_body ON r0c_datagrip_retrieveaccessmethods_app (body);
select A.oid as access_method_id,
       A.xmin as state_number,
       A.amname as access_method_name
       ,
       A.amhandler::oid as handler_id,
       pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name,
       A.amtype as access_method_type
       
from pg_am A
     join pg_proc P on A.amhandler::oid = P.oid
     join pg_namespace N on P.pronamespace = N.oid
     
--  where pg_catalog.age(A.xmin) <= #TXAGE
;
