select umid as id,
       srvid as server_id,
       usename as user,
       umoptions as options
from pg_catalog.pg_user_mappings
order by server_id