-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select N.oid::bigint as id,
       N.xmin as state_number,
       nspname as name,
       D.description,
       pg_catalog.pg_get_userbyid(N.nspowner) as "owner"
from pg_catalog.pg_namespace N
  left join pg_catalog.pg_description D on N.oid = D.objoid
order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end;
