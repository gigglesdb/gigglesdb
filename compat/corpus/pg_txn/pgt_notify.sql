-- case: pgt_notify
-- title: Transactional listen notify unlisten
-- owner: unowned
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Transactional listen notify unlisten
LISTEN r0c_pgt_notify;
BEGIN;
NOTIFY r0c_pgt_notify, 'cache invalidation';
UNLISTEN r0c_pgt_notify;
COMMIT;
LISTEN r0c_pgt_notify;
UNLISTEN *;
NOTIFY r0c_pgt_notify;
