-- case: pgd_ctas_no_data
-- title: ctas no data
-- owner: E1
-- expect: compatible
-- source: Differences: CREATE TABLE AS and SELECT INTO
CREATE TABLE r0c_pgd_ctas_no_data (id int PRIMARY KEY, v int);
INSERT INTO r0c_pgd_ctas_no_data VALUES (1,8);
CREATE TABLE r0c_pgd_ctas_no_data_copy /* PRIMARY KEY added immediately after CTAS */ AS SELECT * FROM r0c_pgd_ctas_no_data WITH NO DATA;
ALTER TABLE r0c_pgd_ctas_no_data_copy ADD PRIMARY KEY(id);
SELECT * FROM r0c_pgd_ctas_no_data_copy ORDER BY id;
DROP TABLE r0c_pgd_ctas_no_data_copy;
