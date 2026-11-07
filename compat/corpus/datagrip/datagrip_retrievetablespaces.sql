-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrievetablespaces
-- title: latest-expanded-queries.json#dg_Batch_RetrieveTablespaces sha256:cf7eb19af49c
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveTablespaces
-- source-sha256: cf7eb19af49c41106336c99bda52cb1db41b71038a6c312aceb767fbf895c146
CREATE TABLE r0c_datagrip_retrievetablespaces_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrievetablespaces_body ON r0c_datagrip_retrievetablespaces_app (body);
select T.oid::bigint as id, T.spcname as name,
       T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,
       pg_catalog.pg_tablespace_location(T.oid) /* null */ as location,
       T.spcoptions /* null */ as options,
       D.description as comment
from pg_catalog.pg_tablespace T
  left join pg_catalog.pg_shdescription D on D.objoid = T.oid
--  where pg_catalog.age(T.xmin) <= #TXAGE
;
