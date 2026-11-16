-- case: pgt_discard_plans
-- title: Discard plans keeps prepared statements usable
-- owner: D
-- expect: compatible
-- source: Differences: Transactions, sessions and connections; W1 / Discard plans keeps prepared statements usable
PREPARE r0c_pgt_discard_plans AS SELECT 42 AS answer;
EXECUTE r0c_pgt_discard_plans;
DISCARD PLANS;
EXECUTE r0c_pgt_discard_plans;
DEALLOCATE r0c_pgt_discard_plans;
