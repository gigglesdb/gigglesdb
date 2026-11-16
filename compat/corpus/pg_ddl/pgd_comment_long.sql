-- case: pgd_comment_long
-- title: comment long
-- owner: G
-- expect: compatible
-- source: Differences: comments longer than 65536 bytes
CREATE TABLE r0c_pgd_comment_long (id int PRIMARY KEY, v int);
CREATE FUNCTION r0c_pgd_comment_long_f() RETURNS void LANGUAGE plpgsql AS $$BEGIN EXECUTE format('COMMENT ON TABLE r0c_pgd_comment_long IS %L',repeat('x',70000)); END$$;
SELECT r0c_pgd_comment_long_f();
SELECT length(obj_description('r0c_pgd_comment_long'::regclass,'pg_class'));
DROP FUNCTION r0c_pgd_comment_long_f();
