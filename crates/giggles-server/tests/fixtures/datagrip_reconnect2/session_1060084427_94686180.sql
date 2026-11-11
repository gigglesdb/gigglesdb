select R.ev_class as table_id,
       R.oid as rule_id,
       R.xmin as rule_state_number,
       R.rulename as rule_name,
       pg_catalog.translate(ev_type,'1234','SUID') as rule_event_code,
       R.ev_enabled as rule_fire_mode,
       R.is_instead as rule_is_instead
from pg_catalog.pg_rewrite R
where R.ev_class in ( select oid
                      from pg_catalog.pg_class
                      where relnamespace = ?::oid
                      --  and relname in ( :[*f_names] )
                    )
  and pg_catalog.age(R.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)
  and R.rulename != '_RETURN'::name
order by R.ev_class::bigint, ev_type