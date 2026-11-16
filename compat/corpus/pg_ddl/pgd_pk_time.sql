-- case: pgd_pk_time
-- title: pk time
-- owner: E1
-- expect: compatible
-- source: Differences: date time interval char primary keys
CREATE TABLE r0c_pgd_pk_time (id time PRIMARY KEY, v text);
INSERT INTO r0c_pgd_pk_time VALUES ('12:30','first'),('13:30','second');
-- @error
INSERT INTO r0c_pgd_pk_time VALUES ('12:30','duplicate');
SELECT * FROM r0c_pgd_pk_time ORDER BY id;
