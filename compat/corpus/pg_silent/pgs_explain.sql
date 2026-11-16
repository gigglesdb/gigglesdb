-- case: pgs_explain
-- title: EXPLAIN COSTS OFF plan output
-- owner: E3
-- expect: compatible
-- source: Differences: Read first / EXPLAIN COSTS OFF plan output
EXPLAIN (COSTS OFF) SELECT 1;
CREATE TABLE r0c_pgs_explain (id int PRIMARY KEY, active boolean);
EXPLAIN (COSTS OFF) SELECT id FROM r0c_pgs_explain WHERE active;
