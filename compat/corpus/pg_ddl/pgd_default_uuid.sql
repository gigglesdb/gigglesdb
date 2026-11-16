-- case: pgd_default_uuid
-- title: default uuid
-- owner: E1
-- expect: compatible
-- source: Differences: arbitrary default expressions
CREATE TABLE r0c_pgd_default_uuid (id int PRIMARY KEY, v uuid DEFAULT gen_random_uuid());
INSERT INTO r0c_pgd_default_uuid (id) VALUES (1);
SELECT id,length(v::text)=36 FROM r0c_pgd_default_uuid ORDER BY id;
