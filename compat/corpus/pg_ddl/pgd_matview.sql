-- case: pgd_matview
-- title: matview
-- owner: E1
-- expect: compatible
-- source: Differences: materialized views
CREATE TABLE r0c_pgd_matview (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_matview VALUES (1,10);
CREATE MATERIALIZED VIEW r0c_pgd_matview_v AS SELECT id,v*2 AS v FROM r0c_pgd_matview;
INSERT INTO r0c_pgd_matview VALUES (2,20);
REFRESH MATERIALIZED VIEW r0c_pgd_matview_v;
SELECT * FROM r0c_pgd_matview_v ORDER BY id;
ALTER MATERIALIZED VIEW r0c_pgd_matview_v RENAME TO r0c_pgd_matview_renamed;
ALTER MATERIALIZED VIEW r0c_pgd_matview_renamed SET (fillfactor=80);
ALTER MATERIALIZED VIEW r0c_pgd_matview_renamed OWNER TO postgres;
DROP MATERIALIZED VIEW r0c_pgd_matview_renamed;
