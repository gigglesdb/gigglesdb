-- case: pgt_two_phase
-- title: Two phase transaction commands under default server config
-- owner: E1
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Two phase transaction commands under default server config
SHOW max_prepared_transactions;
BEGIN;
-- @error
PREPARE TRANSACTION 'r0c_pgt_two_phase';
-- @error
COMMIT PREPARED 'r0c_pgt_two_phase';
-- @error
ROLLBACK PREPARED 'r0c_pgt_two_phase';
