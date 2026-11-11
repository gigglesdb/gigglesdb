select T.oid as object_id,
                 T.relacl as acl
          from pg_catalog.pg_class T
          where relnamespace = ?::oid 
            and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          union all
          select T.oid as object_id,
                 T.proacl as acl
          from pg_catalog.pg_proc T
          where pronamespace = ?::oid 
            and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          union all
          select T.oid as object_id,
                 T.typacl as acl
          from pg_catalog.pg_type T
          where typnamespace = ?::oid 
            and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          order by object_id