-- case: pgp_trigger_statement
-- title: After INSERT STATEMENT audit trigger
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_statement(id integer PRIMARY KEY,v integer);
CREATE TABLE r0c_pgp_trigger_statement_log(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,op text,level text);
CREATE FUNCTION r0c_pgp_trigger_statement_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_statement_log(op,level) VALUES(TG_OP,TG_LEVEL); RETURN NULL; END;$$;
CREATE TRIGGER r0c_pgp_trigger_statement_tr AFTER INSERT ON r0c_pgp_trigger_statement FOR EACH STATEMENT EXECUTE FUNCTION r0c_pgp_trigger_statement_f();
INSERT INTO r0c_pgp_trigger_statement VALUES(1,2),(2,3);
SELECT op,level FROM r0c_pgp_trigger_statement_log ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_statement_tr ON r0c_pgp_trigger_statement;
DROP FUNCTION r0c_pgp_trigger_statement_f();
DROP TABLE r0c_pgp_trigger_statement_log;
DROP TABLE r0c_pgp_trigger_statement;
