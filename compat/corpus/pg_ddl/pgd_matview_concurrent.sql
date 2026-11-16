-- case: pgd_matview_concurrent
-- title: matview concurrent
-- owner: E1
-- expect: compatible
-- source: Differences: materialized views
CREATE TABLE r0c_pgd_matview_concurrent (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_matview_concurrent VALUES (1,10);
CREATE MATERIALIZED VIEW r0c_pgd_matview_concurrent_v AS SELECT id,v*2 AS v FROM r0c_pgd_matview_concurrent;
CREATE UNIQUE INDEX r0c_pgd_matview_concurrent_i ON r0c_pgd_matview_concurrent_v(id);
INSERT INTO r0c_pgd_matview_concurrent VALUES (2,20);
REFRESH MATERIALIZED VIEW CONCURRENTLY r0c_pgd_matview_concurrent_v;
SELECT * FROM r0c_pgd_matview_concurrent_v ORDER BY id;
ALTER MATERIALIZED VIEW r0c_pgd_matview_concurrent_v RENAME TO r0c_pgd_matview_concurrent_renamed;
ALTER MATERIALIZED VIEW r0c_pgd_matview_concurrent_renamed SET (fillfactor=80);
ALTER MATERIALIZED VIEW r0c_pgd_matview_concurrent_renamed OWNER TO postgres;
DROP MATERIALIZED VIEW r0c_pgd_matview_concurrent_renamed;
