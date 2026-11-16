-- case: pgl_large_aggregates
-- title: Integer bigint numeric aggregates over one hundred thousand rows
-- owner: E3
-- expect: compatible
-- source: Maintenance: avg integer and sum avg bigint numeric large non-null input
CREATE TABLE r0c_pgl_large_aggregates (id integer PRIMARY KEY, b bigint, n numeric);
INSERT INTO r0c_pgl_large_aggregates SELECT g, g::bigint, g::numeric FROM generate_series(1, 100000) AS g;
SELECT count(*), avg(id), sum(b), avg(b), sum(n), avg(n) FROM r0c_pgl_large_aggregates;
DROP TABLE r0c_pgl_large_aggregates;
