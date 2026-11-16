-- case: pgq_explain_verbose
-- title: explain verbose
-- owner: E1
-- expect: compatible
-- source: Differences: EXPLAIN ANALYZE; EXPLAIN of data changes
EXPLAIN (VERBOSE, COSTS OFF) SELECT 1 AS constant;
