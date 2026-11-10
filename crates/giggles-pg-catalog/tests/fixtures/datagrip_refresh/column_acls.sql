select T.oid as object_id,
               A.attnum as attr_position,
               A.attacl as acl
        from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid
        where relnamespace = ?::oid
          and attnum > 0 
        order by object_id, attr_position
