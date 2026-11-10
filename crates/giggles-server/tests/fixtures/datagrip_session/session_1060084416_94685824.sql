select TG.oid
from pg_catalog.pg_trigger TG,
     pg_catalog.pg_class TC
where TC.oid = TG.tgrelid
  and TC.relnamespace = ?::oid
