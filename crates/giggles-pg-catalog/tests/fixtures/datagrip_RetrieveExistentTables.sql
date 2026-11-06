select oid from pg_catalog.pg_class C
where relkind in ('r', 'm', 'v', 'p', 'f')
  and relnamespace = $1::oid
;