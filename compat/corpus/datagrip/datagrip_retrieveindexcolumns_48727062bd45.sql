-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveindexcolumns_48727062bd45
-- title: datagrip_RetrieveIndexColumns.sql#datagrip_RetrieveIndexColumns sha256:48727062bd45
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_RetrieveIndexColumns.sql#datagrip_RetrieveIndexColumns
-- source-sha256: 48727062bd45d80888f2b1bf15189b8b2783e6576ed4fa219c6d0ded17715fc7
-- parameters: $1=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrieveindexcolumns_48727062bd45_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveindexcolumns_48727062bd45_body ON r0c_datagrip_retrieveindexcolumns_48727062bd45_app (body);

select ind_head.indexrelid index_id,
       k col_idx,
       k <= indnkeyatts in_key,
       ind_head.indkey[k-1] column_position,
       ind_head.indoption[k-1] column_options,
       ind_head.indcollation[k-1] as collation,
       colln.nspname as collation_schema,
       collname as collation_str,
       ind_head.indclass[k-1] as opclass,
       case when opcdefault then null else opcn.nspname end as opclass_schema,
       case when opcdefault then null else opcname end as opclass_str,
       case
           when indexprs is null then null
           when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)
           else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)
       end as expression,
       amcanorder can_order
from pg_catalog.pg_index ind_head
         join pg_catalog.pg_class ind_stor
              on ind_stor.oid = ind_head.indexrelid
    cross join unnest(ind_head.indkey) with ordinality u(u, k)
         left join pg_catalog.pg_collation
                   on pg_collation.oid = ind_head.indcollation[k-1]
         left join pg_catalog.pg_namespace colln on collnamespace = colln.oid
cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
         left join pg_catalog.pg_opclass
                   on pg_opclass.oid = ind_head.indclass[k-1]
         left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid
  
where ind_stor.relnamespace = 2200::oid
  and ind_stor.relkind in ('i', 'I')

order by index_id, k
;
