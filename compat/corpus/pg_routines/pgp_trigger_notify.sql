-- case: pgp_trigger_notify
-- title: Trigger pg_notify delivers at commit without printing PID
-- owner: unowned
-- expect: compatible
-- source: Differences row 170; W7 triggers calling pg_notify; plain LISTEN commands covered elsewhere
CREATE TABLE r0c_pgp_trigger_notify(id integer PRIMARY KEY);
CREATE FUNCTION r0c_pgp_trigger_notify_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_notify('r0c_pgp_trigger_notify_channel',NEW.id::text); RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_notify_tr AFTER INSERT ON r0c_pgp_trigger_notify FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_notify_f();
INSERT INTO r0c_pgp_trigger_notify VALUES(1);
SELECT id FROM r0c_pgp_trigger_notify ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_notify_tr ON r0c_pgp_trigger_notify;
DROP FUNCTION r0c_pgp_trigger_notify_f();
DROP TABLE r0c_pgp_trigger_notify;
