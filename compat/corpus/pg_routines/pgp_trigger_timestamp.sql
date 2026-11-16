-- case: pgp_trigger_timestamp
-- title: updated_at trigger compares properties only
-- owner: unowned
-- expect: compatible
-- source: Differences row 121; W7 TRIGGER
CREATE TABLE r0c_pgp_trigger_timestamp(id integer PRIMARY KEY,updated_at timestamptz);
INSERT INTO r0c_pgp_trigger_timestamp VALUES(1,'2000-01-01 UTC');
CREATE FUNCTION r0c_pgp_trigger_timestamp_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.updated_at := statement_timestamp(); RETURN NEW; END;$$;
CREATE TRIGGER r0c_pgp_trigger_timestamp_tr BEFORE UPDATE ON r0c_pgp_trigger_timestamp FOR EACH ROW EXECUTE FUNCTION r0c_pgp_trigger_timestamp_f();
UPDATE r0c_pgp_trigger_timestamp SET updated_at=NULL WHERE id=1;
SELECT updated_at IS NOT NULL AS set,updated_at>'2000-01-01 UTC'::timestamptz AS advanced FROM r0c_pgp_trigger_timestamp;
DROP TRIGGER r0c_pgp_trigger_timestamp_tr ON r0c_pgp_trigger_timestamp;
DROP FUNCTION r0c_pgp_trigger_timestamp_f();
DROP TABLE r0c_pgp_trigger_timestamp;
