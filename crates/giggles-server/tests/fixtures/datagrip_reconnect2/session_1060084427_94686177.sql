select oid
from pg_catalog.pg_constraint
where conrelid != 0 and connamespace = ?::oid