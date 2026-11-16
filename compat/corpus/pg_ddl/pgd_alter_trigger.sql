-- case: pgd_alter_trigger
-- title: Alter table trigger enable modes
-- owner: E1
-- expect: compatible
-- source: PG18 ALTER TABLE ENABLE REPLICA ALWAYS TRIGGER
CREATE TABLE r0c_pgd_alter_trigger (id int PRIMARY KEY,v int);
CREATE FUNCTION r0c_pgd_alter_trigger_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v:=NEW.v+1; RETURN NEW; END$$;
CREATE TRIGGER r0c_pgd_alter_trigger_t BEFORE INSERT ON r0c_pgd_alter_trigger FOR EACH ROW EXECUTE FUNCTION r0c_pgd_alter_trigger_f();
ALTER TABLE r0c_pgd_alter_trigger ENABLE REPLICA TRIGGER r0c_pgd_alter_trigger_t;
INSERT INTO r0c_pgd_alter_trigger VALUES(1,10);
ALTER TABLE r0c_pgd_alter_trigger ENABLE ALWAYS TRIGGER r0c_pgd_alter_trigger_t;
INSERT INTO r0c_pgd_alter_trigger VALUES(2,10);
ALTER TABLE r0c_pgd_alter_trigger DISABLE TRIGGER r0c_pgd_alter_trigger_t;
INSERT INTO r0c_pgd_alter_trigger VALUES(3,10);
ALTER TABLE r0c_pgd_alter_trigger ENABLE TRIGGER r0c_pgd_alter_trigger_t;
INSERT INTO r0c_pgd_alter_trigger VALUES(4,10);
SELECT * FROM r0c_pgd_alter_trigger ORDER BY id;
DROP TRIGGER r0c_pgd_alter_trigger_t ON r0c_pgd_alter_trigger;
DROP FUNCTION r0c_pgd_alter_trigger_f();
