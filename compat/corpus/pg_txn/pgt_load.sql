-- case: pgt_load
-- title: Load nonexistent library reports file error
-- owner: unowned
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Load nonexistent library reports file error
-- @error
LOAD 'r0c_pgt_load_missing_library';
