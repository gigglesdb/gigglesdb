select T.tgrelid as table_id,
       T.oid as trigger_id,
       T.xmin as trigger_state_number,
       T.tgname as trigger_name,
       T.tgfoid as function_id,
       pg_catalog.encode(T.tgargs, 'escape') as function_args,
       T.tgtype as bits,
       T.tgdeferrable as is_deferrable,
       T.tginitdeferred as is_init_deferred,
       T.tgenabled as trigger_fire_mode,
       T.tgattr as columns,
       T.tgconstraint != 0 as is_constraint,
       T.tgoldtable /* null */ as old_table_name,
       T.tgnewtable /* null */ as new_table_name,
       pg_catalog.pg_get_triggerdef(T.oid, true) as source_code
from pg_catalog.pg_trigger T
     join pg_catalog.pg_class TAB on TAB.oid = T.tgrelid and TAB.relnamespace = ?::oid
where true
  --  and TAB.relname in ( :[*f_names] )
  --  and pg_catalog.age(T.xmin) <= #TXAGE
  and not T.tgisinternal