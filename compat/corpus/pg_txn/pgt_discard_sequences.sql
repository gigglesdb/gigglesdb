-- case: pgt_discard_sequences
-- title: Discard sequence session state
-- owner: D
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Discard sequence session state
CREATE SEQUENCE r0c_pgt_discard_sequences;
SELECT nextval('r0c_pgt_discard_sequences');
DISCARD SEQUENCES;
-- @error
SELECT currval('r0c_pgt_discard_sequences');
DROP SEQUENCE r0c_pgt_discard_sequences;
