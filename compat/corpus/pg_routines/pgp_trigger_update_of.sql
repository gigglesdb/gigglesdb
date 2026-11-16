-- case: pgp_trigger_update_of
-- title: UPDATE OF fires for named assignment
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_update_of(id integer PRIMARY KEY,v integer,w integer);
INSERT INTO r0c_pgp_trigger_update_of VALUES(1,0,0);
CREATE FUNCTION r0c_pgp_trigger_update_of_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.w := OLD.w+10; RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_update_of_tr BEFORE UPDATE OF v ON r0c_pgp_trigger_update_of FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_update_of_f();
UPDATE r0c_pgp_trigger_update_of SET w=1 WHERE id=1;
SELECT * FROM r0c_pgp_trigger_update_of ORDER BY id;
UPDATE r0c_pgp_trigger_update_of SET v=v WHERE id=1;
SELECT * FROM r0c_pgp_trigger_update_of ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_update_of_tr ON r0c_pgp_trigger_update_of;
DROP FUNCTION r0c_pgp_trigger_update_of_f();
DROP TABLE r0c_pgp_trigger_update_of;
