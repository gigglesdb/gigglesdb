select P.oid
from pg_catalog.pg_policy P
     join pg_catalog.pg_class C on polrelid = C.oid
where relnamespace = ?::oid
