-- case: pgd_comment_function
-- title: comment function
-- owner: G
-- expect: compatible
-- source: Differences: COMMENT ON other object kinds
CREATE FUNCTION r0c_pgd_comment_function() RETURNS int LANGUAGE sql IMMUTABLE AS $$SELECT 1$$;
COMMENT ON FUNCTION r0c_pgd_comment_function() IS 'migration note';
SELECT obj_description('r0c_pgd_comment_function()'::regprocedure,'pg_proc');
COMMENT ON FUNCTION r0c_pgd_comment_function() IS NULL;
SELECT obj_description('r0c_pgd_comment_function()'::regprocedure,'pg_proc') IS NULL AS cleared;
DROP FUNCTION r0c_pgd_comment_function();
