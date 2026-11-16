-- case: pgd_view_security_invoker
-- title: view security invoker
-- owner: E1
-- expect: compatible
-- source: PG18 CREATE VIEW options
CREATE TABLE r0c_pgd_view_security_invoker (id int PRIMARY KEY, v int);
CREATE VIEW r0c_pgd_view_security_invoker_v WITH (security_invoker=true) AS SELECT * FROM r0c_pgd_view_security_invoker WHERE v>0;
INSERT INTO r0c_pgd_view_security_invoker VALUES (1,3);
SELECT * FROM r0c_pgd_view_security_invoker_v ORDER BY id;
ALTER VIEW r0c_pgd_view_security_invoker_v SET (security_invoker=false);
DROP VIEW r0c_pgd_view_security_invoker_v;
