-- case: pgp_trigger_truncate
-- title: After TRUNCATE STATEMENT audit trigger
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_truncate(id integer PRIMARY KEY,v integer);
CREATE TABLE r0c_pgp_trigger_truncate_log(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,op text,level text);
CREATE FUNCTION r0c_pgp_trigger_truncate_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_truncate_log(op,level) VALUES(TG_OP,TG_LEVEL); RETURN NULL; END;$$;
CREATE TRIGGER r0c_pgp_trigger_truncate_tr AFTER TRUNCATE ON r0c_pgp_trigger_truncate FOR EACH STATEMENT EXECUTE FUNCTION r0c_pgp_trigger_truncate_f();
INSERT INTO r0c_pgp_trigger_truncate VALUES(1,2);
TRUNCATE r0c_pgp_trigger_truncate;
SELECT op,level FROM r0c_pgp_trigger_truncate_log ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_truncate_tr ON r0c_pgp_trigger_truncate;
DROP FUNCTION r0c_pgp_trigger_truncate_f();
DROP TABLE r0c_pgp_trigger_truncate_log;
DROP TABLE r0c_pgp_trigger_truncate;
