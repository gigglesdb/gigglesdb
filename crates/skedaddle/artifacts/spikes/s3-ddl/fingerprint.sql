-- Demonstration only: current DB schema + relevant shared catalogs, no user rows or physical file IDs.
-- Includes s3 trigger definitions/enabled state but NOT the schema-version data row.
-- Polling cannot recover the WAL position, intermediate/reverted schemas, TRUNCATE, or data rewrites alone.
WITH ns AS (SELECT oid FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname<>'information_schema'),
objs AS (
 SELECT 'namespace' k,to_jsonb(n) v FROM pg_namespace n WHERE oid IN(SELECT oid FROM ns)
 UNION ALL SELECT 'relation',jsonb_build_array(c.oid,c.relnamespace,c.relname,c.relkind,c.relowner,c.relacl,c.reloptions,c.relreplident,c.relispartition,c.relpartbound::text,CASE WHEN c.relkind='p' THEN pg_get_partkeydef(c.oid) END) FROM pg_class c WHERE relnamespace IN(SELECT oid FROM ns)
 UNION ALL SELECT 'column',jsonb_build_array(a.attrelid,a.attnum,a.attname,a.atttypid,a.atttypmod,a.attcollation,a.attnotnull,a.attisdropped,a.attidentity,a.attgenerated,a.attacl,pg_get_expr(d.adbin,d.adrelid)) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE c.relnamespace IN(SELECT oid FROM ns) AND a.attnum>0
 UNION ALL SELECT 'constraint',jsonb_build_array(c.oid,c.conname,c.conrelid,c.contypid,pg_get_constraintdef(c.oid),c.convalidated) FROM pg_constraint c WHERE c.connamespace IN(SELECT oid FROM ns)
 UNION ALL SELECT 'index',jsonb_build_array(i.indexrelid,pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid WHERE c.relnamespace IN(SELECT oid FROM ns)
 UNION ALL SELECT 'inheritance',to_jsonb(i) FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid WHERE c.relnamespace IN(SELECT oid FROM ns)
 UNION ALL SELECT 'type',to_jsonb(t) FROM pg_type t WHERE typnamespace IN(SELECT oid FROM ns)
 UNION ALL SELECT 'enum',to_jsonb(e) FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typnamespace IN(SELECT oid FROM ns)
 UNION ALL SELECT 'routine',jsonb_build_array(p.oid,p.proowner,p.proacl,pg_get_functiondef(p.oid)) FROM pg_proc p WHERE p.pronamespace IN(SELECT oid FROM ns) AND p.prokind IN('f','p')
 UNION ALL SELECT 'view',jsonb_build_array(c.oid,pg_get_viewdef(c.oid)) FROM pg_class c WHERE c.relnamespace IN(SELECT oid FROM ns) AND c.relkind IN('v','m')
 UNION ALL SELECT 'description',to_jsonb(d) FROM pg_description d
 UNION ALL SELECT 'shared_description',to_jsonb(d) FROM pg_shdescription d
 UNION ALL SELECT 'role',to_jsonb(r)-'rolpassword' FROM pg_roles r
 UNION ALL SELECT 'membership',to_jsonb(m) FROM pg_auth_members m
 UNION ALL SELECT 'database',to_jsonb(d)-'datfrozenxid'-'datminmxid' FROM pg_database d
 UNION ALL SELECT 'tablespace',to_jsonb(t) FROM pg_tablespace t
 UNION ALL SELECT 'event_trigger',to_jsonb(e) FROM pg_event_trigger e
 UNION ALL SELECT 'role_setting',to_jsonb(s) FROM pg_db_role_setting s
 UNION ALL SELECT 'parameter_acl',to_jsonb(p) FROM pg_parameter_acl p
 UNION ALL SELECT 'file_setting',to_jsonb(s) FROM pg_file_settings s
 UNION ALL SELECT 'publication',to_jsonb(p) FROM pg_publication p
 UNION ALL SELECT 'publication_relation',to_jsonb(p) FROM pg_publication_rel p
 UNION ALL SELECT 'default_acl',to_jsonb(a) FROM pg_default_acl a
 UNION ALL SELECT 'policy',to_jsonb(p) FROM pg_policy p
 UNION ALL SELECT 'extension',to_jsonb(e) FROM pg_extension e
)
SELECT md5(coalesce(string_agg(k||':'||v::text,E'\n' ORDER BY k,v::text),'')) FROM objs;
