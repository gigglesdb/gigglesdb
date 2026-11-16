-- case: pgd_pk_date
-- title: pk date
-- owner: E1
-- expect: compatible
-- source: Differences: date time interval char primary keys
CREATE TABLE r0c_pgd_pk_date (id date PRIMARY KEY, v text);
INSERT INTO r0c_pgd_pk_date VALUES ('2024-01-01','first'),('2024-02-01','second');
-- @error
INSERT INTO r0c_pgd_pk_date VALUES ('2024-01-01','duplicate');
SELECT * FROM r0c_pgd_pk_date ORDER BY id;
