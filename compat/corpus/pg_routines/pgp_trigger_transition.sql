-- case: pgp_trigger_transition
-- title: Transition NEW TABLE audit sum
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_transition(id integer PRIMARY KEY,v integer);
CREATE TABLE r0c_pgp_trigger_transition_log(id integer PRIMARY KEY,total bigint);
CREATE FUNCTION r0c_pgp_trigger_transition_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO r0c_pgp_trigger_transition_log SELECT 1,sum(v) FROM new_rows; RETURN NULL; END;$$;
CREATE TRIGGER r0c_pgp_trigger_transition_tr AFTER INSERT ON r0c_pgp_trigger_transition REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION r0c_pgp_trigger_transition_f();
INSERT INTO r0c_pgp_trigger_transition VALUES(1,10),(2,20);
SELECT * FROM r0c_pgp_trigger_transition_log ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_transition_tr ON r0c_pgp_trigger_transition;
DROP FUNCTION r0c_pgp_trigger_transition_f();
DROP TABLE r0c_pgp_trigger_transition_log;
DROP TABLE r0c_pgp_trigger_transition;
