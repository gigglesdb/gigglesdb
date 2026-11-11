select T.oid::bigint as id, T.spcname as name,
       T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,
       pg_catalog.pg_tablespace_location(T.oid) /* null */ as location,
       T.spcoptions /* null */ as options,
       D.description as comment
from pg_catalog.pg_tablespace T
  left join pg_catalog.pg_shdescription D on D.objoid = T.oid
where pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)