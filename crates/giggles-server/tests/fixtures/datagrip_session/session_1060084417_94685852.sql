select D.objoid id, case
    when 'pg_catalog.pg_event_trigger'::regclass = classoid then 'T'
    when 'pg_catalog.pg_am'::regclass = classoid then 'A'
    when 'pg_catalog.pg_cast'::regclass = classoid then 'C'
    when 'pg_catalog.pg_foreign_data_wrapper'::regclass = classoid then 'W'
    when 'pg_catalog.pg_foreign_server'::regclass = classoid then 'S'
    when 'pg_catalog.pg_language'::regclass = classoid then 'L'
    when 'pg_catalog.pg_extension'::regclass = classoid then 'E'
    
  end as kind,
  D.objsubid sub_id, D.description
from pg_catalog.pg_description D
where classoid in (
  'pg_catalog.pg_event_trigger'::regclass,
  'pg_catalog.pg_am'::regclass,
  'pg_catalog.pg_cast'::regclass,
  'pg_catalog.pg_foreign_data_wrapper'::regclass,
  'pg_catalog.pg_foreign_server'::regclass,
  'pg_catalog.pg_language'::regclass
  ,
  'pg_catalog.pg_extension'::regclass
  
)
--  and pg_catalog.age(D.xmin) <= #TXAGE
