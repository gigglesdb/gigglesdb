-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_schemas
-- title: datagrip_schemas.sql#datagrip_schemas sha256:c4a26b438845
-- owner: G
-- expect: compatible
-- compare: shape
-- source: crates/giggles-pg-catalog/tests/fixtures/datagrip_schemas.sql#datagrip_schemas
-- source-sha256: c4a26b438845d4b584680e7428d1f398d22835010810ff244d9fbb8f759b8ae6
CREATE TABLE r0c_datagrip_schemas_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_schemas_body ON r0c_datagrip_schemas_app (body);
-- Extracted from PgIntroQueries.sql sha256 1c544ac583b89d17a43888c4a7ea31d84e817921777bddc78c0fedcf4149549b
select N.oid::bigint as id,
       N.xmin as state_number,
       nspname as name,
       D.description,
       pg_catalog.pg_get_userbyid(N.nspowner) as "owner"
from pg_catalog.pg_namespace N
  left join pg_catalog.pg_description D on N.oid = D.objoid
order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end;
