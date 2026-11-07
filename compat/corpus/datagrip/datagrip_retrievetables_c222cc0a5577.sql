-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrievetables_c222cc0a5577
-- title: datagrip_RetrieveTables.sql#datagrip_RetrieveTables sha256:c222cc0a5577
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_RetrieveTables.sql#datagrip_RetrieveTables
-- source-sha256: c222cc0a5577e0fb796939dbb123c2eff24451bc172b5c531df83ae82c4930ba
-- parameters: $1=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrievetables_c222cc0a5577_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrievetables_c222cc0a5577_body ON r0c_datagrip_retrievetables_c222cc0a5577_app (body);
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
