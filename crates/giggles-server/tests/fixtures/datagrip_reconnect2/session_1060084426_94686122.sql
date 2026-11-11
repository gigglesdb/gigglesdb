select distinct oid, xmin
from pg_catalog.pg_namespace
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_type
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_class
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_proc
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_operator
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_constraint
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_event_trigger
where oid in (2200)
union

select distinct oid, xmin
from pg_catalog.pg_collation
where oid in (2200)
union

select distinct oid, xmin
from pg_catalog.pg_rewrite
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_trigger
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_am
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_cast
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_foreign_data_wrapper
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_language
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_foreign_server
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_opfamily
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_opclass
where oid in (2200)
union
select distinct oid, xmin
from pg_catalog.pg_extension
where oid in (2200)

union
select distinct oid, xmin
from pg_catalog.pg_policy
where oid in (2200)

/* union
select distinct oid, xmin
from pg_catalog.pg_user_mapping
where oid in (2200)
*/