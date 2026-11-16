-- case: pgd_view_security_barrier
-- title: view security barrier
-- owner: E1
-- expect: compatible
-- source: PG18 CREATE VIEW options
CREATE TABLE r0c_pgd_view_security_barrier (id int PRIMARY KEY, v int);
CREATE VIEW r0c_pgd_view_security_barrier_v WITH (security_barrier=true) AS SELECT * FROM r0c_pgd_view_security_barrier WHERE v>0;
INSERT INTO r0c_pgd_view_security_barrier VALUES (1,3);
SELECT * FROM r0c_pgd_view_security_barrier_v ORDER BY id;
ALTER VIEW r0c_pgd_view_security_barrier_v SET (security_barrier=false);
DROP VIEW r0c_pgd_view_security_barrier_v;
