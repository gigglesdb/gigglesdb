select IX.indexrelid
from pg_catalog.pg_index IX,
     pg_catalog.pg_class IC
where IC.oid = IX.indrelid
  and IC.relnamespace = ?::oid
