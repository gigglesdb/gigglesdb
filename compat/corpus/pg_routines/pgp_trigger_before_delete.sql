-- case: pgp_trigger_before_delete
-- title: Row trigger BEFORE DELETE
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_before_delete(id integer PRIMARY KEY,v integer);
CREATE FUNCTION r0c_pgp_trigger_before_delete_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF OLD.v=2 THEN RETURN NULL; END IF; RETURN OLD; END;$$;
CREATE TRIGGER r0c_pgp_trigger_before_delete_tr BEFORE DELETE ON r0c_pgp_trigger_before_delete FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_before_delete_f();
INSERT INTO r0c_pgp_trigger_before_delete VALUES(1,2);
DELETE FROM r0c_pgp_trigger_before_delete WHERE id=1;
SELECT * FROM r0c_pgp_trigger_before_delete ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_before_delete_tr ON r0c_pgp_trigger_before_delete;
DROP FUNCTION r0c_pgp_trigger_before_delete_f();
DROP TABLE r0c_pgp_trigger_before_delete;
