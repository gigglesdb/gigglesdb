-- case: pgf_aggregate_json
-- title: JSON aggregation order NULL and object keys
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-aggregate.html
CREATE TABLE r0c_pgf_aggregate_json (id integer PRIMARY KEY, x integer, y integer, s text, b boolean);
INSERT INTO r0c_pgf_aggregate_json VALUES (1,1,2,'b',true),(2,2,5,'a',false),(3,2,7,'a',NULL),(4,NULL,NULL,NULL,NULL);
SELECT json_agg(x ORDER BY id) FROM r0c_pgf_aggregate_json;
SELECT jsonb_agg(x ORDER BY id) FROM r0c_pgf_aggregate_json;
SELECT json_object_agg(id,s ORDER BY id) FROM r0c_pgf_aggregate_json;
SELECT jsonb_object_agg(id,s ORDER BY id) FROM r0c_pgf_aggregate_json;
SELECT json_agg(DISTINCT x ORDER BY x) FROM r0c_pgf_aggregate_json;
SELECT jsonb_agg(x ORDER BY id) FILTER (WHERE x IS NOT NULL) FROM r0c_pgf_aggregate_json;
SELECT json_agg(x) FROM r0c_pgf_aggregate_json WHERE id>9;
SELECT jsonb_agg(x) FROM r0c_pgf_aggregate_json WHERE id>9;
SELECT json_agg(NULL::integer);
SELECT jsonb_agg(1);
DROP TABLE r0c_pgf_aggregate_json;
