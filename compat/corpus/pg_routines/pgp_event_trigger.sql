-- case: pgp_event_trigger
-- title: DDL end event trigger records command metadata
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 EVENT TRIGGER
CREATE TABLE r0c_pgp_event_trigger_log(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,tag text,kind text);
CREATE FUNCTION r0c_pgp_event_trigger_f() RETURNS event_trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_event_trigger_log(tag,kind) SELECT command_tag,object_type FROM pg_event_trigger_ddl_commands(); END;$$;
CREATE EVENT TRIGGER r0c_pgp_event_trigger_ev ON ddl_command_end WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION r0c_pgp_event_trigger_f();
CREATE TABLE r0c_pgp_event_trigger(id integer PRIMARY KEY);
ALTER EVENT TRIGGER r0c_pgp_event_trigger_ev DISABLE;
SELECT tag,kind FROM r0c_pgp_event_trigger_log ORDER BY tag,kind;
ALTER EVENT TRIGGER r0c_pgp_event_trigger_ev ENABLE;
DROP EVENT TRIGGER r0c_pgp_event_trigger_ev;
DROP FUNCTION r0c_pgp_event_trigger_f();
DROP TABLE r0c_pgp_event_trigger_log;
DROP TABLE r0c_pgp_event_trigger;
