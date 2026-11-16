-- case: pgp_trigger_instead
-- title: INSTEAD OF view trigger writes base table
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_instead(id integer PRIMARY KEY,v integer);
CREATE VIEW r0c_pgp_trigger_instead_v AS SELECT * FROM r0c_pgp_trigger_instead;
CREATE FUNCTION r0c_pgp_trigger_instead_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_instead VALUES(NEW.id,NEW.v+1); RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_instead_tr INSTEAD OF INSERT ON r0c_pgp_trigger_instead_v FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_instead_f();
INSERT INTO r0c_pgp_trigger_instead_v VALUES(1,2);
SELECT * FROM r0c_pgp_trigger_instead ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_instead_tr ON r0c_pgp_trigger_instead_v;
DROP VIEW r0c_pgp_trigger_instead_v;
DROP FUNCTION r0c_pgp_trigger_instead_f();
DROP TABLE r0c_pgp_trigger_instead;
