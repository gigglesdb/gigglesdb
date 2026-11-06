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
  and relnamespace = $1::oid
  and contype in ('p', 'u', 'f', 'c', 'x')
  and connamespace = $1::oid
  
;