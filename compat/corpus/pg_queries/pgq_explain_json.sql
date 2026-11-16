-- case: pgq_explain_json
-- title: explain json
-- owner: E1
-- expect: compatible
-- source: Differences: EXPLAIN ANALYZE; EXPLAIN of data changes
EXPLAIN (FORMAT JSON, COSTS OFF) SELECT 1;
