select pg_amproc.oid
from pg_catalog.pg_amproc
         join pg_catalog.pg_opfamily on pg_opfamily.oid = amprocfamily
where opfnamespace = ?::oid
