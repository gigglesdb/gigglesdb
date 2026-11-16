-- case: pgp_trigger_after_update
-- title: After UPDATE ROW audit trigger
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_after_update(id integer PRIMARY KEY,v integer);
CREATE TABLE r0c_pgp_trigger_after_update_log(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,op text,level text);
CREATE FUNCTION r0c_pgp_trigger_after_update_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_after_update_log(op,level) VALUES(TG_OP,TG_LEVEL); RETURN NULL; END;$$;
CREATE TRIGGER r0c_pgp_trigger_after_update_tr AFTER UPDATE ON r0c_pgp_trigger_after_update FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_after_update_f();
INSERT INTO r0c_pgp_trigger_after_update VALUES(1,2);
UPDATE r0c_pgp_trigger_after_update SET v=3 WHERE id=1;
SELECT op,level FROM r0c_pgp_trigger_after_update_log ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_after_update_tr ON r0c_pgp_trigger_after_update;
DROP FUNCTION r0c_pgp_trigger_after_update_f();
DROP TABLE r0c_pgp_trigger_after_update_log;
DROP TABLE r0c_pgp_trigger_after_update;
