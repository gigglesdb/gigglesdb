-- case: pgp_trigger_before_update
-- title: Row trigger BEFORE UPDATE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_before_update(id integer PRIMARY KEY,v integer);
CREATE FUNCTION r0c_pgp_trigger_before_update_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := OLD.v+NEW.v; RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_before_update_tr BEFORE UPDATE ON r0c_pgp_trigger_before_update FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_before_update_f();
INSERT INTO r0c_pgp_trigger_before_update VALUES(1,2);
UPDATE r0c_pgp_trigger_before_update SET v=3 WHERE id=1;
SELECT * FROM r0c_pgp_trigger_before_update ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_before_update_tr ON r0c_pgp_trigger_before_update;
DROP FUNCTION r0c_pgp_trigger_before_update_f();
DROP TABLE r0c_pgp_trigger_before_update;
