-- case: pgd_comment_subscription
-- title: comment subscription
-- owner: G
-- expect: compatible
-- source: PG18 COMMENT object kinds; W4 COMMENT
CREATE SUBSCRIPTION r0c_pgd_comment_subscription CONNECTION 'host=127.0.0.1 port=1 dbname=postgres' PUBLICATION r0c_pgd_comment_subscription_p WITH(connect=false,slot_name=NONE);
COMMENT ON SUBSCRIPTION r0c_pgd_comment_subscription IS 'catalog note';
SELECT shobj_description((SELECT oid FROM pg_subscription WHERE subname='r0c_pgd_comment_subscription'),'pg_subscription');
COMMENT ON SUBSCRIPTION r0c_pgd_comment_subscription IS NULL;
SELECT shobj_description((SELECT oid FROM pg_subscription WHERE subname='r0c_pgd_comment_subscription'),'pg_subscription') IS NULL AS cleared;
DROP SUBSCRIPTION r0c_pgd_comment_subscription;
