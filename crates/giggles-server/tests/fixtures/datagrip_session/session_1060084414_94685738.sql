select srv.oid as id,
       srv.srvfdw as fdw_id,
       srv.xmin as state_number,
       srv.srvname as name,
       srv.srvtype as type,
       srv.srvversion as version,
       srv.srvoptions as options,
       pg_catalog.pg_get_userbyid(srv.srvowner) as "owner"
from pg_catalog.pg_foreign_server srv
--  where pg_catalog.age(srv.xmin) <= #TXAGE
