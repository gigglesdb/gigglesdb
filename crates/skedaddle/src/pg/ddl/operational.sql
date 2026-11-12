WITH scope AS (
 SELECT s.schema_name,s.table_name,c.* FROM unnest($1::text[], $2::text[]) s(schema_name,table_name)
 LEFT JOIN pg_namespace n ON n.nspname=s.schema_name
 LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=s.table_name
), descriptors AS (
 SELECT s.schema_name,s.table_name,jsonb_build_array(
 s.schema_name,s.table_name,s.oid,s.relkind,s.relreplident,s.relispartition,
 pg_get_expr(s.relpartbound,s.oid),pg_get_partkeydef(s.oid),
 (SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attcollation,a.attnotnull,a.attisdropped,a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
  FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=s.oid AND a.attnum>0),
 (SELECT jsonb_agg(jsonb_build_array(c.conname,pg_get_constraintdef(c.oid),c.convalidated) ORDER BY c.conname) FROM pg_constraint c WHERE c.conrelid=s.oid),
 (SELECT jsonb_agg(jsonb_build_array(ic.relname,pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indisprimary,i.indisunique) ORDER BY ic.relname) FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid WHERE i.indrelid=s.oid),
 (SELECT jsonb_agg(jsonb_build_array(cn.nspname,c.relname,pn.nspname,p.relname,h.inhseqno,h.inhdetachpending) ORDER BY cn.nspname,c.relname,pn.nspname,p.relname,h.inhseqno)
 FROM pg_inherits h JOIN pg_class c ON c.oid=h.inhrelid JOIN pg_namespace cn ON cn.oid=c.relnamespace JOIN pg_class p ON p.oid=h.inhparent JOIN pg_namespace pn ON pn.oid=p.relnamespace WHERE h.inhrelid=s.oid OR h.inhparent=s.oid)
 ) AS descriptor FROM scope s
)
SELECT md5(COALESCE(jsonb_agg(descriptor ORDER BY schema_name,table_name)::text,'[]')) FROM descriptors
