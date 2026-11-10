select pronamespace as schemaId,
       oid as majorOid,
       proargnames as argNames,
       proargmodes as argModes,
       array_length(proargtypes, 1) as nArgs
from pg_catalog.pg_proc
where pronamespace in ( ? )
  --  and pg_catalog.age(xmin) <= #TXAGE
order by schemaId
