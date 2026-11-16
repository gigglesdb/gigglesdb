-- case: pgd_ctas
-- title: ctas
-- owner: E1
-- expect: compatible
-- source: Differences: CREATE TABLE AS and SELECT INTO
CREATE TABLE r0c_pgd_ctas (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_ctas VALUES (1,8);
CREATE TABLE r0c_pgd_ctas_copy /* PRIMARY KEY added immediately after CTAS */ AS SELECT * FROM r0c_pgd_ctas;
ALTER TABLE r0c_pgd_ctas_copy ADD PRIMARY KEY(id);
SELECT * FROM r0c_pgd_ctas_copy ORDER BY id;
DROP TABLE r0c_pgd_ctas_copy;
