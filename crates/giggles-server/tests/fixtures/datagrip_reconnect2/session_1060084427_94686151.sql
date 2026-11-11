select D.objoid id
from pg_catalog.pg_description D
where D.classoid in (
  'pg_catalog.pg_event_trigger'::regclass,
  'pg_catalog.pg_am'::regclass,
  'pg_catalog.pg_cast'::regclass,
  'pg_catalog.pg_foreign_data_wrapper'::regclass,
  'pg_catalog.pg_foreign_server'::regclass,
  'pg_catalog.pg_language'::regclass
  ,
  'pg_catalog.pg_extension'::regclass
  
)