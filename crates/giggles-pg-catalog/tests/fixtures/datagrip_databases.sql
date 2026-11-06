-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select N.oid::bigint as id,
       datname as name,
       D.description,
       datistemplate as is_template,
       datallowconn as allow_connections,
       pg_catalog.pg_get_userbyid(N.datdba) as "owner"
from pg_catalog.pg_database N
  left join pg_catalog.pg_shdescription D on N.oid = D.objoid
order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end;
