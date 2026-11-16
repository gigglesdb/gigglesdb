-- case: pgp_trigger_constraint
-- title: Deferrable constraint trigger waits until SET CONSTRAINTS
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_constraint(id integer PRIMARY KEY,v integer);
CREATE TABLE r0c_pgp_trigger_constraint_log(id integer PRIMARY KEY);
CREATE FUNCTION r0c_pgp_trigger_constraint_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_constraint_log VALUES(NEW.id); RETURN NULL; END;$$;
CREATE CONSTRAINT TRIGGER r0c_pgp_trigger_constraint_tr AFTER INSERT ON r0c_pgp_trigger_constraint DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_constraint_f();
BEGIN;
INSERT INTO r0c_pgp_trigger_constraint VALUES(1,2);
SELECT count(*) FROM r0c_pgp_trigger_constraint_log;
SET CONSTRAINTS ALL IMMEDIATE;
SELECT count(*) FROM r0c_pgp_trigger_constraint_log;
COMMIT;
DROP TRIGGER r0c_pgp_trigger_constraint_tr ON r0c_pgp_trigger_constraint;
DROP FUNCTION r0c_pgp_trigger_constraint_f();
DROP TABLE r0c_pgp_trigger_constraint_log;
DROP TABLE r0c_pgp_trigger_constraint;
