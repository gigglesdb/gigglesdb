-- case: pgp_trigger_toggle
-- title: Enable disable and rename a trigger
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_toggle(id integer PRIMARY KEY,v integer);
CREATE FUNCTION r0c_pgp_trigger_toggle_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+1; RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_toggle_tr BEFORE INSERT ON r0c_pgp_trigger_toggle FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_toggle_f();
ALTER TRIGGER r0c_pgp_trigger_toggle_tr ON r0c_pgp_trigger_toggle RENAME TO r0c_pgp_trigger_toggle_renamed;
ALTER TABLE r0c_pgp_trigger_toggle DISABLE TRIGGER r0c_pgp_trigger_toggle_renamed;
INSERT INTO r0c_pgp_trigger_toggle VALUES(1,0);
ALTER TABLE r0c_pgp_trigger_toggle ENABLE TRIGGER r0c_pgp_trigger_toggle_renamed;
INSERT INTO r0c_pgp_trigger_toggle VALUES(2,0);
SELECT * FROM r0c_pgp_trigger_toggle ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_toggle_renamed ON r0c_pgp_trigger_toggle;
DROP FUNCTION r0c_pgp_trigger_toggle_f();
DROP TABLE r0c_pgp_trigger_toggle;
