-- case: pgp_trigger_after_delete
-- title: After DELETE ROW audit trigger
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_after_delete(id integer PRIMARY KEY,v integer);
CREATE TABLE r0c_pgp_trigger_after_delete_log(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,op text,level text);
CREATE FUNCTION r0c_pgp_trigger_after_delete_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_after_delete_log(op,level) VALUES(TG_OP,TG_LEVEL); RETURN NULL; END;$$;
CREATE TRIGGER r0c_pgp_trigger_after_delete_tr AFTER DELETE ON r0c_pgp_trigger_after_delete FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_after_delete_f();
INSERT INTO r0c_pgp_trigger_after_delete VALUES(1,2);
DELETE FROM r0c_pgp_trigger_after_delete WHERE id=1;
SELECT op,level FROM r0c_pgp_trigger_after_delete_log ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_after_delete_tr ON r0c_pgp_trigger_after_delete;
DROP FUNCTION r0c_pgp_trigger_after_delete_f();
DROP TABLE r0c_pgp_trigger_after_delete_log;
DROP TABLE r0c_pgp_trigger_after_delete;
