CREATE SCHEMA s3;
CREATE TABLE s3.schema_version(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),version bigint NOT NULL);
INSERT INTO s3.schema_version VALUES(true,0);
CREATE FUNCTION s3.ddl_boundary() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,s3 AS $$
DECLARE details jsonb; v bigint;
BEGIN
  SELECT coalesce(jsonb_agg(jsonb_build_object(
    'classid',d.classid,'objid',d.objid,'objsubid',d.objsubid,
    'command_tag',d.command_tag,'object_type',d.object_type,
    'schema_name',d.schema_name,'object_identity',d.object_identity,'in_extension',d.in_extension,
    'columns',(SELECT jsonb_agg(jsonb_build_object('attnum',a.attnum,'name',a.attname,
       'type',format_type(a.atttypid,a.atttypmod),'notnull',a.attnotnull,'dropped',a.attisdropped,
       'identity',a.attidentity,'generated',a.attgenerated,'missing',a.atthasmissing,'missing_value',a.attmissingval::text,
       'default',pg_get_expr(ad.adbin,ad.adrelid)) ORDER BY a.attnum)
       FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
       WHERE d.classid='pg_class'::regclass AND a.attrelid=d.objid AND a.attnum>0),
    'relation',(SELECT jsonb_build_object('kind',c.relkind,'partition',c.relispartition,'filenode',c.relfilenode,
       'constraints',(SELECT coalesce(jsonb_agg(pg_get_constraintdef(k.oid) ORDER BY k.oid),'[]'::jsonb) FROM pg_constraint k WHERE k.conrelid=c.oid),
       'parents',(SELECT coalesce(jsonb_agg(i.inhparent ORDER BY i.inhseqno),'[]'::jsonb) FROM pg_inherits i WHERE i.inhrelid=c.oid))
       FROM pg_class c WHERE d.classid='pg_class'::regclass AND c.oid=d.objid)
    ) ORDER BY d.classid,d.objid,d.objsubid),'[]'::jsonb)
    INTO details FROM pg_event_trigger_ddl_commands() d;
  UPDATE s3.schema_version SET version=version+1 RETURNING version INTO v;
  PERFORM pg_logical_emit_message(true,'skedaddle',jsonb_build_object(
    'version',v,'tag',TG_TAG,'xid',txid_current()::text,'query',current_query(),'commands',details)::text);
END $$;
CREATE EVENT TRIGGER s3_boundary ON ddl_command_end EXECUTE FUNCTION s3.ddl_boundary();
