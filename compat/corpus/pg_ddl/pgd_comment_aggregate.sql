-- case: pgd_comment_aggregate
-- title: comment aggregate
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE AGGREGATE r0c_pgd_comment_aggregate(int) (SFUNC=int4pl,STYPE=int,INITCOND='0');
COMMENT ON AGGREGATE r0c_pgd_comment_aggregate(int) IS 'catalog note';
SELECT obj_description('r0c_pgd_comment_aggregate(int)'::regprocedure,'pg_proc');
COMMENT ON AGGREGATE r0c_pgd_comment_aggregate(int) IS NULL;
SELECT obj_description('r0c_pgd_comment_aggregate(int)'::regprocedure,'pg_proc') IS NULL AS cleared;
DROP AGGREGATE r0c_pgd_comment_aggregate(int);
