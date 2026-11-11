select fdw.oid as id,
       fdw.xmin as state_number,
       fdw.fdwname as name,
       pr.proname as handler,
       nspc.nspname as handler_schema,
       pr2.proname as validator,
       nspc2.nspname as validator_schema,
       fdw.fdwoptions as options,
       pg_catalog.pg_get_userbyid(fdw.fdwowner) as "owner"
from pg_catalog.pg_foreign_data_wrapper fdw
     left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid
     left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid
     left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid
     left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid
--  where pg_catalog.age(fdw.xmin) <= #TXAGE