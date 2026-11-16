-- case: pgd_generated_virtual
-- title: generated virtual
-- owner: E1
-- expect: compatible
-- source: Differences: generated columns
CREATE TABLE r0c_pgd_generated_virtual (id int PRIMARY KEY, price int, qty int, total int GENERATED ALWAYS AS (price*qty) VIRTUAL);
INSERT INTO r0c_pgd_generated_virtual (id,price,qty) VALUES (1,12,3);
UPDATE r0c_pgd_generated_virtual SET qty=4;
SELECT * FROM r0c_pgd_generated_virtual ORDER BY id;
-- @error
INSERT INTO r0c_pgd_generated_virtual VALUES (2,1,1,99);
