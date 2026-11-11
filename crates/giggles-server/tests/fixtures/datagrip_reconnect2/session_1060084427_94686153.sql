select T.oid as object_id,
                 T.fdwacl as acl
          from pg_catalog.pg_foreign_data_wrapper T 
            where pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          union all
          select T.oid as object_id,
                 T.lanacl as acl
          from pg_catalog.pg_language T 
            where pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          union all
          select T.oid as object_id,
                 T.nspacl as acl
          from pg_catalog.pg_namespace T 
            where pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          union all
          select T.oid as object_id,
                 T.srvacl as acl
          from pg_catalog.pg_foreign_server T 
            where pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          