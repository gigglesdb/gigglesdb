-- case: aggregate_filter
-- title: Aggregate filter
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_aggregate_filter_t (id int);
INSERT INTO r0c_aggregate_filter_t VALUES (1);
INSERT INTO r0c_aggregate_filter_t VALUES (2);
SELECT count(*) FILTER (WHERE id > 1) FROM r0c_aggregate_filter_t;
