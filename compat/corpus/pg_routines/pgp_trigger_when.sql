-- case: pgp_trigger_when
-- title: Trigger WHEN condition filters rows
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_when(id integer PRIMARY KEY,v integer);
CREATE FUNCTION r0c_pgp_trigger_when_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+100; RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_when_tr BEFORE INSERT ON r0c_pgp_trigger_when FOR EACH ROW WHEN (NEW.v>0) EXECUTE FUNCTION r0c_pgp_trigger_when_f();
INSERT INTO r0c_pgp_trigger_when VALUES(1,1),(2,0);
SELECT * FROM r0c_pgp_trigger_when ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_when_tr ON r0c_pgp_trigger_when;
DROP FUNCTION r0c_pgp_trigger_when_f();
DROP TABLE r0c_pgp_trigger_when;
