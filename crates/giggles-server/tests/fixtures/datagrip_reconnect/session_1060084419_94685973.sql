with T as ( select /* distinct */ T.oid as oid,
                                     T.relkind as kind,
                                     T.relnamespace as schemaId
            from pg_catalog.pg_class T
              --  , pg_catalog.pg_attribute A
            where T.relnamespace in ( ? )
              and T.relkind in ('r', 'm', 'v', 'f', 'p')
              --  and (pg_catalog.age(A.xmin) <= #TXAGE or pg_catalog.age(T.xmin) <= #TXAGE)
              --  and A.attrelid = T.oid
          )
select T.schemaId as schemaId,
       T.oid as majorOid,
       pg_catalog.translate(T.kind, 'rmvpf', 'rmvrf') as kind,
       C.attnum as position,
       C.attname as name
from T
     join pg_catalog.pg_attribute C on T.oid = C.attrelid
where C.attnum > 0
  and not C.attisdropped
order by schemaId, majorOid