select T.oid as oid,
       relnamespace as schemaId,
       pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind,
       relname as name
from pg_catalog.pg_class T
where relnamespace in ( ? )
  and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
  --  and pg_catalog.age(T.xmin) <= #TXAGE
-- end relations (table like stuff & seq)
union all
-- begin objectType
select T.oid,
       T.typnamespace,
       'T',
       T.typname
from pg_catalog.pg_type T
     left outer join pg_catalog.pg_class C on T.typrelid = C.oid
where T.typnamespace in ( ? )
  and ( T.typtype in ('d','e') or
        C.relkind = 'c'::"char" or
        (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
        T.typtype = 'p' and not T.typisdefined )
  --  and pg_catalog.age(T.xmin) <= #TXAGE
--
union all
-- begin collation
select oid,
       collnamespace,
       'C',
       collname
from pg_catalog.pg_collation
where collnamespace in ( ? )
  --  and pg_catalog.age(xmin) <= #TXAGE
--
union all
-- begin ops
select oid,
       oprnamespace,
       'O',
       oprname
from pg_catalog.pg_operator
where oprnamespace in ( ? )
  --  and pg_catalog.age(xmin) <= #TXAGE
--
union all
-- begin opClasses
select oid,
       opcnamespace,
       'c',
       opcname
from pg_catalog.pg_opclass
where opcnamespace in ( ? )
  --  and pg_catalog.age(xmin) <= #TXAGE
--
union all
-- begin opFams
select oid,
       opfnamespace,
       'F',
       opfname
from pg_catalog.pg_opfamily
where opfnamespace in ( ? )
  --  and pg_catalog.age(xmin) <= #TXAGE
--
union all
-- begin agg & routine
select oid,
       pronamespace,
       case when prokind != 'a'/* not proisagg*/ then 'R'
            else 'a'
            end,
       proname
from pg_catalog.pg_proc
where pronamespace in ( ? )
  --  and pg_catalog.age(xmin) <= #TXAGE
--
order by schemaId