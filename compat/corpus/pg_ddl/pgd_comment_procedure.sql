-- case: pgd_comment_procedure
-- title: comment procedure
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE PROCEDURE r0c_pgd_comment_procedure() LANGUAGE sql AS $$SELECT 1$$;
COMMENT ON PROCEDURE r0c_pgd_comment_procedure() IS 'migration note';
SELECT obj_description('r0c_pgd_comment_procedure()'::regprocedure,'pg_proc');
COMMENT ON PROCEDURE r0c_pgd_comment_procedure() IS NULL;
SELECT obj_description('r0c_pgd_comment_procedure()'::regprocedure,'pg_proc') IS NULL AS cleared;
DROP PROCEDURE r0c_pgd_comment_procedure();
