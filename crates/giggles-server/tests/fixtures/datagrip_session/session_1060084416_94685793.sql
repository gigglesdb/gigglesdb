select T.oid as object_id,
                 T.fdwacl as acl
          from pg_catalog.pg_foreign_data_wrapper T 
          union all
          select T.oid as object_id,
                 T.lanacl as acl
          from pg_catalog.pg_language T 
          union all
          select T.oid as object_id,
                 T.nspacl as acl
          from pg_catalog.pg_namespace T 
          union all
          select T.oid as object_id,
                 T.srvacl as acl
          from pg_catalog.pg_foreign_server T 
          
