-- generated-by: compat/tools/gen_datagrip_corpus.py
-- case: datagrip_role_memberships
-- title: latest-expanded-queries.json#dg_Batch_RetrieveRoleGrants sha256:6cc3b48585aa
-- owner: G
-- expect: compatible
-- compare: header
-- source: crates/giggles-pg-conformance/probes/datagrip-2026.2.5/latest-expanded-queries.json#dg_Batch_RetrieveRoleGrants
-- source-sha256: 6cc3b48585aa59fbb9c42ec1b39f91f12983942525c106979df4b27823f4ec46
CREATE TABLE r0c_datagrip_role_memberships_app (id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea);
CREATE INDEX datagrip_role_memberships_body ON r0c_datagrip_role_memberships_app (body);
select member id, roleid role_id, admin_option
          from pg_catalog.pg_auth_members order by id, roleid::text
;
