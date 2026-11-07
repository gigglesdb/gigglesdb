select T.oid as object_id,
                 T.relacl as acl
          from pg_catalog.pg_class T
          where relnamespace = $1::oid 
          union all
          select T.oid as object_id,
                 T.proacl as acl
          from pg_catalog.pg_proc T
          where pronamespace = $2::oid 
          union all
          select T.oid as object_id,
                 T.typacl as acl
          from pg_catalog.pg_type T
          where typnamespace = $3::oid 
          order by object_id