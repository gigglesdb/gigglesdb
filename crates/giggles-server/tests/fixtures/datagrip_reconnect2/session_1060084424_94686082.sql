select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted,
       h.proname as handler, hs.nspname as handlerSchema,
       i.proname as inline, isc.nspname as inlineSchema,
       v.proname as validator, vs.nspname as validatorSchema
from pg_catalog.pg_language l
    left join pg_catalog.pg_proc h on h.oid = lanplcallfoid
    left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace
    left join pg_catalog.pg_proc i on i.oid = laninline
    left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace
    left join pg_catalog.pg_proc v on v.oid = lanvalidator
    left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace
where pg_catalog.age(l.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)
order by lanname