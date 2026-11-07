CREATE SCHEMA s3b;
CREATE SEQUENCE s3b.command_seq;
CREATE FUNCTION s3b.boundary() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,s3b AS $$
DECLARE cid text; helper_error text; helper_rows bigint; details jsonb; phase text;
BEGIN
  IF TG_EVENT='ddl_command_start' THEN
    -- Coarse fail-closed fallback: deliberately block the whole tag family.
    -- No SQL regex is trusted to decide whether a command can split transactions.
    IF current_setting('s3b.strict',true)='on' AND TG_TAG IN ('ALTER TABLE','CREATE INDEX') THEN
      RAISE EXCEPTION 'SKEDADDLE strict: DDL class blocked before execution; split-transaction forms not permitted (tag=%)', TG_TAG;
    END IF;
    cid := pg_backend_pid()::text || ':' || nextval('s3b.command_seq')::text;
    PERFORM set_config('s3b.command_id',cid,false);
    BEGIN
      SELECT count(*) INTO helper_rows FROM pg_event_trigger_ddl_commands();
    EXCEPTION WHEN OTHERS THEN helper_error := SQLSTATE || ': ' || SQLERRM;
    END;
    phase := 'start'; details := '[]'::jsonb;
  ELSE
    cid := current_setting('s3b.command_id',true);
    phase := 'finish';
    SELECT coalesce(jsonb_agg(jsonb_build_object('classid',classid,'objid',objid,'objsubid',objsubid,
      'command_tag',command_tag,'object_type',object_type,'object_identity',object_identity)),'[]'::jsonb)
      INTO details FROM pg_event_trigger_ddl_commands();
  END IF;
  PERFORM pg_logical_emit_message(true,'skedaddle',jsonb_build_object(
    'ddl',phase,'id',cid,'pid',pg_backend_pid(),'tag',TG_TAG,'xid',txid_current()::text,
    'query',current_query(),'start_helper_error',helper_error,'start_helper_rows',helper_rows,'commands',details)::text);
END $$;
CREATE EVENT TRIGGER s3b_start ON ddl_command_start EXECUTE FUNCTION s3b.boundary();
CREATE EVENT TRIGGER s3b_finish ON ddl_command_end EXECUTE FUNCTION s3b.boundary();
