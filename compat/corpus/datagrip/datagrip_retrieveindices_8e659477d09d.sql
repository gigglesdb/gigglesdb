-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveindices_8e659477d09d
-- title: datagrip_RetrieveIndices.sql#datagrip_RetrieveIndices sha256:8e659477d09d
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_RetrieveIndices.sql#datagrip_RetrieveIndices
-- source-sha256: 8e659477d09de326976e7de66b07b0172fffda2dcc3bf77d5e1662f643e8ba9b
-- parameters: $1=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrieveindices_8e659477d09d_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveindices_8e659477d09d_body ON r0c_datagrip_retrieveindices_8e659477d09d_app (body);
select tab.oid               table_id,
       tab.relkind           table_kind,
       ind_stor.relname      index_name,
       ind_head.indexrelid   index_id,
       ind_stor.xmin         state_number,
       ind_head.indisunique  is_unique,
       ind_head.indisprimary is_primary,
       false nulls_not_distinct,
       pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition,
       (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors,
       ind_stor.reltablespace tablespace_id,
       opcmethod as access_method_id
from pg_catalog.pg_class tab
         join pg_catalog.pg_index ind_head
              on ind_head.indrelid = tab.oid
         join pg_catalog.pg_class ind_stor
              on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid
         left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)
where tab.relnamespace = 2200::oid
        and tab.relkind in ('r', 'm', 'v', 'p')
        and ind_stor.relkind in ('i', 'I')


;
