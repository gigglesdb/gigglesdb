select pg_amop.oid
from pg_catalog.pg_amop
         join pg_catalog.pg_opfamily on pg_opfamily.oid = amopfamily
where opfnamespace = ?::oid
