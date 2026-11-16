-- case: pgp_trigger_before_insert
-- title: Row trigger BEFORE INSERT
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_before_insert(id integer PRIMARY KEY,v integer);
CREATE FUNCTION r0c_pgp_trigger_before_insert_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+10; RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_before_insert_tr BEFORE INSERT ON r0c_pgp_trigger_before_insert FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_before_insert_f();
INSERT INTO r0c_pgp_trigger_before_insert VALUES(1,2);
SELECT * FROM r0c_pgp_trigger_before_insert ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_before_insert_tr ON r0c_pgp_trigger_before_insert;
DROP FUNCTION r0c_pgp_trigger_before_insert_f();
DROP TABLE r0c_pgp_trigger_before_insert;
