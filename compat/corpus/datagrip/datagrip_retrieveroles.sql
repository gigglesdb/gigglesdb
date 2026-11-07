-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_retrieveroles
-- title: latest-expanded-queries.json#dg_Batch_RetrieveRoles sha256:d2c91bcaf2b3
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveRoles
-- source-sha256: d2c91bcaf2b389a2b23b65162cf63f789c1d0f781609c7c9d943dea2bf0dcd8b
CREATE TABLE r0c_datagrip_retrieveroles_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_retrieveroles_body ON r0c_datagrip_retrieveroles_app (body);
select R.oid::bigint as role_id, rolname as role_name,
  rolsuper is_super, rolinherit is_inherit,
  rolcreaterole can_createrole, rolcreatedb can_createdb,
  rolcanlogin can_login, rolreplication /* false */ is_replication,
  rolconnlimit conn_limit, rolvaliduntil valid_until,
  rolbypassrls /* false */ bypass_rls, rolconfig config,
  D.description
from pg_catalog.pg_roles R
  left join pg_catalog.pg_shdescription D on D.objoid = R.oid
;
