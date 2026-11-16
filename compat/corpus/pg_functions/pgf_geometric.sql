-- case: pgf_geometric
-- title: Point box operators and geometry functions
-- owner: DD-0068
-- expect: compatible
-- source: Differences Functions and operators; functions.txt; https://www.postgresql.org/docs/18/functions-geometry.html
CREATE TABLE r0c_pgf_geometric (id integer PRIMARY KEY, p point, b box);
INSERT INTO r0c_pgf_geometric VALUES (1,'(1,2)','((0,0),(4,4))'),(2,'(5,6)','((1,1),(2,2))'),(3,NULL,NULL);
SELECT id, p <-> point '(0,0)' FROM r0c_pgf_geometric ORDER BY id;
SELECT id, b @> p FROM r0c_pgf_geometric ORDER BY id;
SELECT id, area(b) FROM r0c_pgf_geometric ORDER BY id;
SELECT id, center(b) FROM r0c_pgf_geometric ORDER BY id;
SELECT id, b && box '((3,3),(5,5))' FROM r0c_pgf_geometric ORDER BY id;
SELECT id, width(b) FROM r0c_pgf_geometric ORDER BY id;
SELECT id, height(b) FROM r0c_pgf_geometric ORDER BY id;
SELECT id, p + point '(1,1)' FROM r0c_pgf_geometric ORDER BY id;
SELECT id, box(point '(0,0)',point '(2,2)') FROM r0c_pgf_geometric ORDER BY id;
DROP TABLE r0c_pgf_geometric;
