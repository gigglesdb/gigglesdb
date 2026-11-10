select A.oid as access_method_id,
       A.xmin as state_number,
       A.amname as access_method_name
       ,
       A.amhandler::oid as handler_id,
       pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name,
       A.amtype as access_method_type
       
from pg_am A
     join pg_proc P on A.amhandler::oid = P.oid
     join pg_namespace N on P.pronamespace = N.oid
     
--  where pg_catalog.age(A.xmin) <= #TXAGE
