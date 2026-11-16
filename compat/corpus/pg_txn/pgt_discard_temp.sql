-- case: pgt_discard_temp
-- title: Discard temporary tables
-- owner: D
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Discard temporary tables
CREATE TEMP TABLE r0c_pgt_discard_temp (id int PRIMARY KEY);
INSERT INTO r0c_pgt_discard_temp VALUES (1);
DISCARD TEMP;
-- @error
SELECT id FROM r0c_pgt_discard_temp;
