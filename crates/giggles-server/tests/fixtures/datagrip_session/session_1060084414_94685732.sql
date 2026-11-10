select T.oid as object_id,
                 T.spcacl as acl
          from pg_catalog.pg_tablespace T 
          union all
          select T.oid as object_id,
                 T.datacl as acl
          from pg_catalog.pg_database T 
