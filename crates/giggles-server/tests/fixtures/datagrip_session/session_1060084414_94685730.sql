select member id, roleid role_id, admin_option
          from pg_catalog.pg_auth_members order by id, roleid::text
