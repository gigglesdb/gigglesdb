-- case: pgd_subscription
-- title: subscription
-- owner: G
-- expect: compatible
-- source: W4 CREATE ALTER DROP SUBSCRIPTION connect false
CREATE SUBSCRIPTION r0c_pgd_subscription CONNECTION 'host=127.0.0.1 port=1 dbname=postgres' PUBLICATION r0c_pgd_subscription_pub WITH(connect=false);
ALTER SUBSCRIPTION r0c_pgd_subscription SET (slot_name=NONE);
ALTER SUBSCRIPTION r0c_pgd_subscription DISABLE;
ALTER SUBSCRIPTION r0c_pgd_subscription RENAME TO r0c_pgd_subscription_new;
ALTER SUBSCRIPTION r0c_pgd_subscription_new OWNER TO postgres;
SELECT subname,subenabled,subslotname IS NULL FROM pg_subscription WHERE subname='r0c_pgd_subscription_new';
DROP SUBSCRIPTION r0c_pgd_subscription_new;
