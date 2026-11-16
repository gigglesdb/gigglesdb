-- case: pgd_comment_cast
-- title: comment cast
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE TYPE r0c_pgd_comment_cast AS ENUM('a');
CREATE CAST (r0c_pgd_comment_cast AS text) WITH INOUT;
COMMENT ON CAST (r0c_pgd_comment_cast AS text) IS 'catalog note';
SELECT obj_description((SELECT oid FROM pg_cast WHERE castsource='r0c_pgd_comment_cast'::regtype AND casttarget='text'::regtype),'pg_cast');
COMMENT ON CAST (r0c_pgd_comment_cast AS text) IS NULL;
SELECT obj_description((SELECT oid FROM pg_cast WHERE castsource='r0c_pgd_comment_cast'::regtype AND casttarget='text'::regtype),'pg_cast') IS NULL AS cleared;
DROP CAST (r0c_pgd_comment_cast AS text);
DROP TYPE r0c_pgd_comment_cast;
