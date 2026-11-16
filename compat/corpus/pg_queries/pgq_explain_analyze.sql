-- case: pgq_explain_analyze
-- title: explain analyze
-- owner: E1
-- expect: compatible
-- source: Differences: EXPLAIN ANALYZE; EXPLAIN of data changes
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT 1;
