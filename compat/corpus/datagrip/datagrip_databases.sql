-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_databases
-- title: datagrip_databases.sql#datagrip_databases sha256:cc153e7793e8
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_databases.sql#datagrip_databases
-- source-sha256: cc153e7793e8b63fbb81ab598b3aaeaeda188035fc3489f6edde53c915cf907f
CREATE TABLE r0c_datagrip_databases_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_databases_body ON r0c_datagrip_databases_app (body);
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
