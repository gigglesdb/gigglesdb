-- case: aggregate_over_app_table
-- title: Aggregate over app table
-- owner: E3
-- expect: compatible
CREATE TABLE r0c_aggregate_over_app_table_t (id int PRIMARY KEY);
INSERT INTO r0c_aggregate_over_app_table_t VALUES (1);
INSERT INTO r0c_aggregate_over_app_table_t VALUES (2);
SELECT count(*), sum(id) FROM r0c_aggregate_over_app_table_t;
