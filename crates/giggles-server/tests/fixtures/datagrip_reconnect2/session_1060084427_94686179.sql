select RU.oid
from pg_catalog.pg_rewrite RU,
     pg_catalog.pg_class RC
where RC.oid = RU.ev_class
  and RC.relnamespace = ?::oid
  and not RU.rulename = '_RETURN'