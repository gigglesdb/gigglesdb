-- case: pgd_generated_stored
-- title: generated stored
-- owner: E1
-- expect: compatible
-- source: Differences: generated columns
CREATE TABLE r0c_pgd_generated_stored (id int PRIMARY KEY, price int, qty int, total int GENERATED ALWAYS AS (price*qty) STORED);
INSERT INTO r0c_pgd_generated_stored (id,price,qty) VALUES (1,12,3);
UPDATE r0c_pgd_generated_stored SET qty=4;
SELECT * FROM r0c_pgd_generated_stored ORDER BY id;
-- @error
INSERT INTO r0c_pgd_generated_stored VALUES (2,1,1,99);
