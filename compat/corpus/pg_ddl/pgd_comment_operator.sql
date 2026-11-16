-- case: pgd_comment_operator
-- title: comment operator
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE SCHEMA r0c_pgd_comment_operator;
CREATE FUNCTION r0c_pgd_comment_operator_eq(int,int) RETURNS bool LANGUAGE sql IMMUTABLE AS $$SELECT $1=$2$$;
CREATE OPERATOR r0c_pgd_comment_operator.=== (LEFTARG=int,RIGHTARG=int,FUNCTION=r0c_pgd_comment_operator_eq);
COMMENT ON OPERATOR r0c_pgd_comment_operator.=== (int,int) IS 'catalog note';
SELECT obj_description((SELECT o.oid FROM pg_operator o JOIN pg_namespace n ON n.oid=o.oprnamespace WHERE n.nspname='r0c_pgd_comment_operator' AND o.oprname='==='),'pg_operator');
COMMENT ON OPERATOR r0c_pgd_comment_operator.=== (int,int) IS NULL;
SELECT obj_description((SELECT o.oid FROM pg_operator o JOIN pg_namespace n ON n.oid=o.oprnamespace WHERE n.nspname='r0c_pgd_comment_operator' AND o.oprname='==='),'pg_operator') IS NULL AS cleared;
DROP OPERATOR r0c_pgd_comment_operator.=== (int,int);
DROP FUNCTION r0c_pgd_comment_operator_eq(int,int);
DROP SCHEMA r0c_pgd_comment_operator;
