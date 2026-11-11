with A as ( select oid as table_id,
                   pg_catalog.upper(relkind) as table_kind
            from pg_catalog.pg_class
            where relnamespace = ?::oid
              and relkind in ('r', 'm', 'v', 'f', 'p')
              --  and relname in ( :[*f_names] )
          )
select table_kind,
       table_id,
       R.oid as rule_id,
       pg_catalog.pg_get_ruledef(R.oid, true) as source_text
from A
     join pg_catalog.pg_rewrite R
       on A.table_id = R.ev_class
where R.rulename != '_RETURN'::name
  --  and pg_catalog.age(R.xmin) <= #SRCTXAGE