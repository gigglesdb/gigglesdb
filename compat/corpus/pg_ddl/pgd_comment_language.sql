-- case: pgd_comment_language
-- title: comment language
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT ON LANGUAGE
CREATE LANGUAGE r0c_pgd_comment_language HANDLER plpgsql_call_handler INLINE plpgsql_inline_handler VALIDATOR plpgsql_validator;
COMMENT ON LANGUAGE r0c_pgd_comment_language IS 'language alias';
SELECT obj_description((SELECT oid FROM pg_language WHERE lanname='r0c_pgd_comment_language'),'pg_language');
COMMENT ON LANGUAGE r0c_pgd_comment_language IS NULL;
SELECT obj_description((SELECT oid FROM pg_language WHERE lanname='r0c_pgd_comment_language'),'pg_language') IS NULL;
DROP LANGUAGE r0c_pgd_comment_language;
