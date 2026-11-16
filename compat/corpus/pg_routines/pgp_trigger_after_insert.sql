-- case: pgp_trigger_after_insert
-- title: After INSERT ROW audit trigger
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_after_insert(id integer PRIMARY KEY,v integer);
CREATE TABLE r0c_pgp_trigger_after_insert_log(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,op text,level text);
CREATE FUNCTION r0c_pgp_trigger_after_insert_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_after_insert_log(op,level) VALUES(TG_OP,TG_LEVEL); RETURN NULL; END;$$;
CREATE TRIGGER r0c_pgp_trigger_after_insert_tr AFTER INSERT ON r0c_pgp_trigger_after_insert FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_after_insert_f();
INSERT INTO r0c_pgp_trigger_after_insert VALUES(1,2);
SELECT op,level FROM r0c_pgp_trigger_after_insert_log ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_after_insert_tr ON r0c_pgp_trigger_after_insert;
DROP FUNCTION r0c_pgp_trigger_after_insert_f();
DROP TABLE r0c_pgp_trigger_after_insert_log;
DROP TABLE r0c_pgp_trigger_after_insert;
