select oid as id,
       umserver as server_id,
       case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as user,
       umoptions as options
from pg_catalog.pg_user_mapping
where pg_catalog.age(xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)
order by server_id