-- case: pgd_comment_trigger
-- title: comment trigger
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE TABLE r0c_pgd_comment_trigger (id int PRIMARY KEY);
CREATE FUNCTION r0c_pgd_comment_trigger_f() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
CREATE TRIGGER r0c_pgd_comment_trigger_t BEFORE INSERT ON r0c_pgd_comment_trigger FOR EACH ROW EXECUTE FUNCTION r0c_pgd_comment_trigger_f();
COMMENT ON TRIGGER r0c_pgd_comment_trigger_t ON r0c_pgd_comment_trigger IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_trigger WHERE tgname='r0c_pgd_comment_trigger_t'),'pg_trigger');
COMMENT ON TRIGGER r0c_pgd_comment_trigger_t ON r0c_pgd_comment_trigger IS NULL;
SELECT obj_description((SELECT oid FROM pg_trigger WHERE tgname='r0c_pgd_comment_trigger_t'),'pg_trigger') IS NULL AS cleared;
DROP TRIGGER r0c_pgd_comment_trigger_t ON r0c_pgd_comment_trigger;
DROP FUNCTION r0c_pgd_comment_trigger_f();
