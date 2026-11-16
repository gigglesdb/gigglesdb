-- case: pgd_comment_event_trigger
-- title: comment event trigger
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE FUNCTION r0c_pgd_comment_event_trigger_f() RETURNS event_trigger LANGUAGE plpgsql AS $$BEGIN RETURN; END$$;
CREATE EVENT TRIGGER r0c_pgd_comment_event_trigger ON ddl_command_end EXECUTE FUNCTION r0c_pgd_comment_event_trigger_f();
COMMENT ON EVENT TRIGGER r0c_pgd_comment_event_trigger IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_event_trigger WHERE evtname='r0c_pgd_comment_event_trigger'),'pg_event_trigger');
COMMENT ON EVENT TRIGGER r0c_pgd_comment_event_trigger IS NULL;
SELECT obj_description((SELECT oid FROM pg_event_trigger WHERE evtname='r0c_pgd_comment_event_trigger'),'pg_event_trigger') IS NULL AS cleared;
DROP EVENT TRIGGER r0c_pgd_comment_event_trigger;
DROP FUNCTION r0c_pgd_comment_event_trigger_f();
