-- case: pgd_comment_routine
-- title: comment routine
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE FUNCTION r0c_pgd_comment_routine() RETURNS int LANGUAGE sql AS $$SELECT 3$$;
COMMENT ON ROUTINE r0c_pgd_comment_routine() IS 'catalog note';
SELECT obj_description('r0c_pgd_comment_routine()'::regprocedure,'pg_proc');
COMMENT ON ROUTINE r0c_pgd_comment_routine() IS NULL;
SELECT obj_description('r0c_pgd_comment_routine()'::regprocedure,'pg_proc') IS NULL AS cleared;
DROP FUNCTION r0c_pgd_comment_routine();
