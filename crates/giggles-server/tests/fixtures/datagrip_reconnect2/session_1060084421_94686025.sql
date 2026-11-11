select T.oid as oid,
       relnamespace as schemaId,
       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind
from pg_catalog.pg_class T
where relnamespace in ( null )
  and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
--
union all
-- begin objectType
select T.oid,
       T.typnamespace,
       'T' as kind
from pg_catalog.pg_type T
     left outer join pg_catalog.pg_class C on T.typrelid = C.oid
where T.typnamespace in ( null )
  and ( T.typtype in ('d','e') or
        C.relkind = 'c'::"char" or
        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
        T.typtype = 'p' and not T.typisdefined )
--
union all
-- begin collation
select oid,
       collnamespace,
       'C' as kind
from pg_catalog.pg_collation
where collnamespace in ( null )
--
union all
-- begin ops
select oid,
       oprnamespace,
       'O' as kind
from pg_catalog.pg_operator
where oprnamespace in ( null )
--
union all
-- begin opClasses
select oid,
       opcnamespace,
       'c' as kind
from pg_catalog.pg_opclass
where opcnamespace in ( null )
--
union all
-- begin opFams
select oid,
       opfnamespace,
       'F' as kind
from pg_catalog.pg_opfamily
where opfnamespace in ( null )
--
union all
-- begin agg & routine
select oid,
       pronamespace,
       case when prokind != 'a' /* not proisagg */ then 'R'
            else 'a'
            end as kind
from pg_catalog.pg_proc
where pronamespace in ( null )