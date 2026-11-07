-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrievetables
-- title: app-expanded-queries.json#dg_App_RetrieveTables sha256:331148333441
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/app-expanded-queries.json#dg_App_RetrieveTables
-- source-sha256: 331148333441b5b8cf9a9c679089d46c9b0347dd375cca268352b70706cc6a71
-- parameters: :schema_id=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrievetables_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrievetables_body ON r0c_datagrip_retrievetables_app (body);
select T.relkind as table_kind,
       T.relname as table_name,
       T.oid as table_id,
       T.xmin as table_state_number,
       false as table_with_oids,
       T.reltablespace as tablespace_id,
       T.reloptions as options,
       T.relpersistence as persistence,
       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors,
       (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors,
       T.relispartition as is_partition,
       pg_catalog.pg_get_partkeydef(T.oid) as partition_key,
       pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression,
       T.relam am_id,
       pg_catalog.pg_get_userbyid(T.relowner) as "owner"
from pg_catalog.pg_class T
where relnamespace = 2200::oid
       and relkind in ('r', 'm', 'v', 'f', 'p')


order by table_kind, table_id
;
