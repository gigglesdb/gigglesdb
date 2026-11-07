-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveconstraints_9df0a4e5953e
-- title: datagrip_RetrieveConstraints.sql#datagrip_RetrieveConstraints sha256:9df0a4e5953e
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_RetrieveConstraints.sql#datagrip_RetrieveConstraints
-- source-sha256: 9df0a4e5953e2b9eb05e5d7104cb5c549160a4c54584dd333ad82df5b477d615
-- parameters: $1=2200 (literal public namespace OID).
CREATE TABLE r0c_datagrip_retrieveconstraints_9df0a4e5953e_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveconstraints_9df0a4e5953e_body ON r0c_datagrip_retrieveconstraints_9df0a4e5953e_app (body);
select T.oid table_id,
       relkind table_kind,
       C.oid::bigint con_id,
       C.xmin::varchar::bigint con_state_id,
       conname con_name,
       contype con_kind,
       conkey con_columns,
       conindid index_id,
       confrelid ref_table_id,
       condeferrable is_deferrable,
       condeferred is_init_deferred,
       false is_not_enforced,
       false is_not_validated,
       false is_defined_with_period,
       confupdtype on_update,
       confdeltype on_delete,
       confmatchtype match_type,
        connoinherit no_inherit,
       pg_catalog.pg_get_expr(conbin, T.oid) con_expression,
       confkey ref_columns,
       conexclop::int[] excl_operators,
       array(select unnest::regoper::varchar from unnest(conexclop)) excl_operators_str
from pg_catalog.pg_constraint C
         join pg_catalog.pg_class T
              on C.conrelid = T.oid
where relkind in ('r', 'v', 'f', 'p')
  and relnamespace = 2200::oid
  and contype in ('p', 'u', 'f', 'c', 'x')
  and connamespace = 2200::oid
  
;
