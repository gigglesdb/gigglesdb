-- case: pgp_trigger_replica
-- title: Replica role suppresses normal trigger enables replica trigger
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_replica(id integer PRIMARY KEY,v integer);
CREATE FUNCTION r0c_pgp_trigger_replica_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := NEW.v+1; RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_replica_tr BEFORE INSERT ON r0c_pgp_trigger_replica FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_replica_f();
SET session_replication_role=replica;
INSERT INTO r0c_pgp_trigger_replica VALUES(1,0);
ALTER TABLE r0c_pgp_trigger_replica ENABLE REPLICA TRIGGER r0c_pgp_trigger_replica_tr;
INSERT INTO r0c_pgp_trigger_replica VALUES(2,0);
RESET session_replication_role;
SELECT * FROM r0c_pgp_trigger_replica ORDER BY id;
DROP TRIGGER r0c_pgp_trigger_replica_tr ON r0c_pgp_trigger_replica;
DROP FUNCTION r0c_pgp_trigger_replica_f();
DROP TABLE r0c_pgp_trigger_replica;
