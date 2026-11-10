select E.oid        as id,
       E.xmin       as state_number,
       extname      as name,
       extversion   as version,
       extnamespace as schema_id,
       nspname      as schema_name
       ,
       array(select unnest
             from unnest(available_versions)
             where unnest > extversion) as available_updates
       
from pg_catalog.pg_extension E
       join pg_namespace N on E.extnamespace = N.oid
       left join (select name, array_agg(version) as available_versions
                  from pg_available_extension_versions()
                  group by name) V on E.extname = V.name
       
--  where pg_catalog.age(E.xmin) <= #TXAGE
