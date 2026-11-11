select T.oid as object_id,
                 T.spcacl as acl
          from pg_catalog.pg_tablespace T 
            where pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
          union all
          select T.oid as object_id,
                 T.datacl as acl
          from pg_catalog.pg_database T 
            where pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)